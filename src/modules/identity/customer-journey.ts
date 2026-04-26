import type { PoolClient } from 'pg';

type DbClient = PoolClient;

function normalizeJourneyIds(journeyIds: string[]): string[] {
  return [...new Set(journeyIds.map((journeyId) => journeyId.trim()).filter(Boolean))].sort();
}

export async function refreshCustomerJourneyForJourneys(client: DbClient, journeyIds: string[]): Promise<void> {
  const normalizedJourneyIds = normalizeJourneyIds(journeyIds);

  if (normalizedJourneyIds.length === 0) {
    return;
  }

  await client.query('SELECT pg_advisory_xact_lock($1)', [918_440_12]);
  await client.query(
    `
      DELETE FROM customer_journey
      WHERE identity_journey_id = ANY($1::uuid[])
    `,
    [normalizedJourneyIds]
  );

  await client.query(
    `
      WITH scoped_sessions AS (
        SELECT
          s.id AS session_id,
          s.identity_journey_id,
          s.anonymous_user_id,
          s.first_seen_at,
          s.last_seen_at,
          s.updated_at AS session_updated_at,
          COALESCE(s.landing_page, captured.landing_url) AS landing_page,
          COALESCE(s.referrer_url, captured.referrer_url) AS referrer_url,
          COALESCE(s.initial_utm_source, captured.initial_utm_source) AS utm_source,
          COALESCE(s.initial_utm_medium, captured.initial_utm_medium) AS utm_medium,
          COALESCE(s.initial_utm_campaign, captured.initial_utm_campaign) AS utm_campaign,
          COALESCE(s.initial_utm_content, captured.initial_utm_content) AS utm_content,
          COALESCE(s.initial_utm_term, captured.initial_utm_term) AS utm_term,
          COALESCE(s.initial_gclid, captured.initial_gclid) AS gclid,
          COALESCE(s.initial_gbraid, captured.initial_gbraid) AS gbraid,
          COALESCE(s.initial_wbraid, captured.initial_wbraid) AS wbraid,
          COALESCE(s.initial_fbclid, captured.initial_fbclid) AS fbclid,
          COALESCE(s.initial_ttclid, captured.initial_ttclid) AS ttclid,
          COALESCE(s.initial_msclkid, captured.initial_msclkid) AS msclkid,
          captured.updated_at AS identity_capture_updated_at,
          journey.authoritative_shopify_customer_id,
          journey.primary_email_hash,
          journey.primary_phone_hash,
          journey.status AS journey_status,
          journey.merge_version AS journey_merge_version,
          journey.created_at AS journey_created_at,
          journey.updated_at AS journey_updated_at,
          journey.last_resolved_at AS journey_last_resolved_at,
          journey.lookback_window_started_at AS journey_lookback_window_started_at,
          journey.lookback_window_expires_at AS journey_lookback_window_expires_at,
          journey.last_touch_eligible_at AS journey_last_touch_eligible_at
        FROM tracking_sessions s
        INNER JOIN identity_journeys journey
          ON journey.id = s.identity_journey_id
        LEFT JOIN session_attribution_identities captured
          ON captured.roas_radar_session_id = s.id
        WHERE s.identity_journey_id = ANY($1::uuid[])
          AND s.first_seen_at >= journey.lookback_window_started_at
          AND s.first_seen_at <= journey.lookback_window_expires_at
      ),
      event_rollup AS (
        SELECT
          e.session_id,
          MIN(e.occurred_at) AS first_event_at,
          MAX(e.occurred_at) AS last_event_at,
          COUNT(*)::int AS session_event_count,
          COUNT(*) FILTER (WHERE e.event_type = 'page_view')::int AS page_view_count,
          COUNT(*) FILTER (WHERE e.event_type = 'product_view')::int AS product_view_count,
          COUNT(*) FILTER (WHERE e.event_type = 'add_to_cart')::int AS add_to_cart_count,
          COUNT(*) FILTER (WHERE e.event_type = 'checkout_started')::int AS checkout_started_count,
          ARRAY_REMOVE(ARRAY_AGG(DISTINCT e.shopify_checkout_token), NULL) AS checkout_tokens,
          ARRAY_REMOVE(ARRAY_AGG(DISTINCT e.shopify_cart_token), NULL) AS cart_tokens
        FROM tracking_events e
        INNER JOIN scoped_sessions s
          ON s.session_id = e.session_id
        GROUP BY e.session_id
      ),
      session_orders AS (
        SELECT DISTINCT
          s.session_id,
          s.identity_journey_id,
          o.shopify_order_id,
          o.total_price,
          COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at,
          COALESCE(o.updated_at_shopify, o.processed_at, o.created_at_shopify, o.ingested_at) AS order_updated_at
        FROM scoped_sessions s
        LEFT JOIN event_rollup e
          ON e.session_id = s.session_id
        INNER JOIN shopify_orders o
          ON o.identity_journey_id = s.identity_journey_id
         AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= s.journey_lookback_window_started_at
         AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= s.journey_lookback_window_expires_at
         AND (
           o.landing_session_id = s.session_id
           OR (
             o.checkout_token IS NOT NULL
             AND e.checkout_tokens IS NOT NULL
             AND o.checkout_token = ANY(e.checkout_tokens)
           )
           OR (
             o.cart_token IS NOT NULL
             AND e.cart_tokens IS NOT NULL
             AND o.cart_token = ANY(e.cart_tokens)
           )
         )
      ),
      session_order_rollup AS (
        SELECT
          o.session_id,
          COUNT(*)::int AS session_order_count,
          COALESCE(SUM(o.total_price), 0)::numeric(12, 2) AS session_order_revenue,
          MIN(o.order_occurred_at) AS session_first_order_at,
          MAX(o.order_occurred_at) AS session_last_order_at,
          MAX(o.order_updated_at) AS latest_order_updated_at
        FROM session_orders o
        GROUP BY o.session_id
      ),
      journey_order_rollup AS (
        SELECT
          o.identity_journey_id,
          COUNT(*)::int AS journey_order_count,
          COALESCE(SUM(o.total_price), 0)::numeric(12, 2) AS journey_order_revenue,
          MIN(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) AS journey_first_order_at,
          MAX(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) AS journey_last_order_at
        FROM shopify_orders o
        INNER JOIN identity_journeys journey
          ON journey.id = o.identity_journey_id
        WHERE o.identity_journey_id = ANY($1::uuid[])
          AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= journey.lookback_window_started_at
          AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= journey.lookback_window_expires_at
        GROUP BY o.identity_journey_id
      ),
      ranked_sessions AS (
        SELECT
          s.*,
          e.first_event_at,
          e.last_event_at,
          COALESCE(e.session_event_count, 0) AS session_event_count,
          COALESCE(e.page_view_count, 0) AS page_view_count,
          COALESCE(e.product_view_count, 0) AS product_view_count,
          COALESCE(e.add_to_cart_count, 0) AS add_to_cart_count,
          COALESCE(e.checkout_started_count, 0) AS checkout_started_count,
          COALESCE(o.session_order_count, 0) AS session_order_count,
          COALESCE(o.session_order_revenue, 0)::numeric(12, 2) AS session_order_revenue,
          o.session_first_order_at,
          o.session_last_order_at,
          o.latest_order_updated_at,
          COALESCE(j.journey_order_count, 0) AS journey_order_count,
          COALESCE(j.journey_order_revenue, 0)::numeric(12, 2) AS journey_order_revenue,
          j.journey_first_order_at,
          j.journey_last_order_at,
          COALESCE(e.first_event_at, s.first_seen_at) AS session_started_at,
          GREATEST(s.last_seen_at, COALESCE(e.last_event_at, s.last_seen_at), s.first_seen_at) AS session_ended_at
        FROM scoped_sessions s
        LEFT JOIN event_rollup e
          ON e.session_id = s.session_id
        LEFT JOIN session_order_rollup o
          ON o.session_id = s.session_id
        LEFT JOIN journey_order_rollup j
          ON j.identity_journey_id = s.identity_journey_id
      ),
      numbered_sessions AS (
        SELECT
          s.*,
          ROW_NUMBER() OVER (
            PARTITION BY s.identity_journey_id
            ORDER BY s.session_started_at ASC, s.session_id ASC
          ) AS journey_session_number,
          COUNT(*) OVER (
            PARTITION BY s.identity_journey_id
          ) AS journey_session_count,
          MIN(s.session_started_at) OVER (
            PARTITION BY s.identity_journey_id
          ) AS journey_started_at,
          MAX(s.session_ended_at) OVER (
            PARTITION BY s.identity_journey_id
          ) AS journey_ended_at,
          SUM(s.session_event_count) OVER (
            PARTITION BY s.identity_journey_id
          ) AS journey_event_count,
          SUM(s.session_event_count) OVER (
            PARTITION BY s.identity_journey_id
            ORDER BY s.session_started_at ASC, s.session_id ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS journey_event_end_number
        FROM ranked_sessions s
      )
      INSERT INTO customer_journey (
        session_id,
        identity_journey_id,
        authoritative_shopify_customer_id,
        primary_email_hash,
        primary_phone_hash,
        journey_status,
        journey_merge_version,
        journey_created_at,
        journey_last_resolved_at,
        journey_lookback_window_started_at,
        journey_lookback_window_expires_at,
        journey_last_touch_eligible_at,
        journey_started_at,
        journey_ended_at,
        journey_session_number,
        reverse_journey_session_number,
        journey_session_count,
        journey_event_start_number,
        journey_event_end_number,
        journey_event_count,
        journey_order_count,
        journey_order_revenue,
        journey_first_order_at,
        journey_last_order_at,
        session_started_at,
        session_ended_at,
        first_event_at,
        last_event_at,
        session_event_count,
        page_view_count,
        product_view_count,
        add_to_cart_count,
        checkout_started_count,
        session_order_count,
        session_order_revenue,
        session_first_order_at,
        session_last_order_at,
        is_first_session,
        is_last_session,
        is_converting_session,
        anonymous_user_id,
        landing_page,
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        gclid,
        gbraid,
        wbraid,
        fbclid,
        ttclid,
        msclkid,
        source_max_updated_at,
        refreshed_at
      )
      SELECT
        s.session_id,
        s.identity_journey_id,
        s.authoritative_shopify_customer_id,
        s.primary_email_hash,
        s.primary_phone_hash,
        s.journey_status,
        s.journey_merge_version,
        s.journey_created_at,
        s.journey_last_resolved_at,
        s.journey_lookback_window_started_at,
        s.journey_lookback_window_expires_at,
        s.journey_last_touch_eligible_at,
        s.journey_started_at,
        s.journey_ended_at,
        s.journey_session_number,
        s.journey_session_count - s.journey_session_number + 1 AS reverse_journey_session_number,
        s.journey_session_count,
        CASE
          WHEN s.session_event_count > 0 THEN s.journey_event_end_number - s.session_event_count + 1
          ELSE 0
        END AS journey_event_start_number,
        s.journey_event_end_number,
        s.journey_event_count,
        s.journey_order_count,
        s.journey_order_revenue,
        s.journey_first_order_at,
        s.journey_last_order_at,
        s.session_started_at,
        s.session_ended_at,
        s.first_event_at,
        s.last_event_at,
        s.session_event_count,
        s.page_view_count,
        s.product_view_count,
        s.add_to_cart_count,
        s.checkout_started_count,
        s.session_order_count,
        s.session_order_revenue,
        s.session_first_order_at,
        s.session_last_order_at,
        s.journey_session_number = 1 AS is_first_session,
        s.journey_session_count = s.journey_session_number AS is_last_session,
        s.session_order_count > 0 AS is_converting_session,
        s.anonymous_user_id,
        s.landing_page,
        s.referrer_url,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        s.utm_content,
        s.utm_term,
        s.gclid,
        s.gbraid,
        s.wbraid,
        s.fbclid,
        s.ttclid,
        s.msclkid,
        GREATEST(
          s.session_updated_at,
          s.journey_updated_at,
          COALESCE(s.identity_capture_updated_at, s.session_updated_at),
          COALESCE(s.last_event_at, s.session_updated_at),
          COALESCE(s.latest_order_updated_at, s.session_updated_at)
        ) AS source_max_updated_at,
        now()
      FROM numbered_sessions s
    `,
    [normalizedJourneyIds]
  );
}
