import { type PoolClient } from 'pg';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import {
  ATTRIBUTION_MODELS,
  type AttributionCredit,
  type AttributionModel,
  type AttributionTouchpoint,
  computeAttributionOutputs
} from './engine.js';

type PendingOrder = {
  shopify_order_id: string;
  order_occurred_at: Date | null;
  created_at_shopify: Date | null;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  customer_identity_id: string | null;
  total_price: string;
};

type SessionTouchpointRow = {
  session_id: string;
  touchpoint_occurred_at: Date;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attributed_content: string | null;
  attributed_term: string | null;
  attributed_click_id_type: string | null;
  attributed_click_id_value: string | null;
};

type SessionEvidence = {
  reason: string;
  rank: number;
  forced: boolean;
};

type LegacyAttributionResult = {
  sessionId: string | null;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  confidenceScore: string;
  reason: string;
};

function uniqueSessionIds(sessionIds: Array<string | null | undefined>): string[] {
  return [...new Set(sessionIds.filter((sessionId): sessionId is string => Boolean(sessionId)))];
}

function isTaggedTouchpoint(row: SessionTouchpointRow): boolean {
  return Boolean(
    row.attributed_click_id_value ||
      row.attributed_source ||
      row.attributed_medium ||
      row.attributed_campaign ||
      row.attributed_content ||
      row.attributed_term
  );
}

function isDirectTouchpoint(row: SessionTouchpointRow): boolean {
  const source = row.attributed_source?.trim().toLowerCase() ?? '';
  const medium = row.attributed_medium?.trim().toLowerCase() ?? '';

  if (row.attributed_click_id_value) {
    return false;
  }

  if (isTaggedTouchpoint(row)) {
    return source === 'direct' || source === '(direct)' || medium === 'none' || medium === '(none)';
  }

  return true;
}

function confidenceForReason(reason: string): string {
  switch (reason) {
    case 'matched_by_landing_session_id':
    case 'matched_by_checkout_token':
      return '1.00';
    case 'matched_by_cart_token':
      return '0.90';
    case 'matched_by_customer_identity':
      return '0.60';
    default:
      return '0.00';
  }
}

async function fetchSessionIdsByCheckoutToken(
  client: PoolClient,
  checkoutToken: string,
  orderOccurredAt: Date | null
): Promise<string[]> {
  const result = await client.query<{ session_id: string }>(
    `
      SELECT DISTINCT e.session_id
      FROM tracking_events e
      WHERE e.shopify_checkout_token = $1
        AND ($2::timestamptz IS NULL OR e.occurred_at >= $2::timestamptz - ($3::int * interval '1 day'))
        AND ($2::timestamptz IS NULL OR e.occurred_at <= $2::timestamptz)
      ORDER BY e.session_id
    `,
    [checkoutToken, orderOccurredAt, env.ATTRIBUTION_WINDOW_DAYS]
  );

  return result.rows.map((row) => row.session_id);
}

async function fetchSessionIdsByCartToken(
  client: PoolClient,
  cartToken: string,
  orderOccurredAt: Date | null
): Promise<string[]> {
  const result = await client.query<{ session_id: string }>(
    `
      SELECT DISTINCT e.session_id
      FROM tracking_events e
      WHERE e.shopify_cart_token = $1
        AND ($2::timestamptz IS NULL OR e.occurred_at >= $2::timestamptz - ($3::int * interval '1 day'))
        AND ($2::timestamptz IS NULL OR e.occurred_at <= $2::timestamptz)
      ORDER BY e.session_id
    `,
    [cartToken, orderOccurredAt, env.ATTRIBUTION_WINDOW_DAYS]
  );

  return result.rows.map((row) => row.session_id);
}

async function fetchIdentitySessionIds(
  client: PoolClient,
  customerIdentityId: string,
  orderOccurredAt: Date | null
): Promise<string[]> {
  const result = await client.query<{ session_id: string }>(
    `
      SELECT s.id AS session_id
      FROM tracking_sessions s
      WHERE s.customer_identity_id = $1::uuid
        AND ($2::timestamptz IS NULL OR s.first_seen_at >= $2::timestamptz - ($3::int * interval '1 day'))
        AND ($2::timestamptz IS NULL OR s.first_seen_at <= $2::timestamptz)
      ORDER BY COALESCE(s.last_seen_at, s.first_seen_at) ASC, s.id ASC
    `,
    [customerIdentityId, orderOccurredAt, env.ATTRIBUTION_WINDOW_DAYS]
  );

  return result.rows.map((row) => row.session_id);
}

async function fetchTouchpointsBySessionIds(
  client: PoolClient,
  sessionIds: string[],
  orderOccurredAt: Date | null
): Promise<SessionTouchpointRow[]> {
  if (sessionIds.length === 0) {
    return [];
  }

  const result = await client.query<SessionTouchpointRow>(
    `
      SELECT
        s.id AS session_id,
        COALESCE(latest_event.occurred_at, s.last_seen_at, s.first_seen_at) AS touchpoint_occurred_at,
        s.initial_utm_source AS attributed_source,
        s.initial_utm_medium AS attributed_medium,
        s.initial_utm_campaign AS attributed_campaign,
        s.initial_utm_content AS attributed_content,
        s.initial_utm_term AS attributed_term,
        CASE
          WHEN COALESCE(latest_event.gclid, s.initial_gclid) IS NOT NULL THEN 'gclid'
          WHEN COALESCE(latest_event.fbclid, s.initial_fbclid) IS NOT NULL THEN 'fbclid'
          WHEN COALESCE(latest_event.ttclid, s.initial_ttclid) IS NOT NULL THEN 'ttclid'
          WHEN COALESCE(latest_event.msclkid, s.initial_msclkid) IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS attributed_click_id_type,
        COALESCE(
          latest_event.gclid,
          latest_event.fbclid,
          latest_event.ttclid,
          latest_event.msclkid,
          s.initial_gclid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS attributed_click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT
          e.occurred_at,
          e.gclid,
          e.fbclid,
          e.ttclid,
          e.msclkid
        FROM tracking_events e
        WHERE e.session_id = s.id
          AND ($2::timestamptz IS NULL OR e.occurred_at <= $2::timestamptz)
        ORDER BY e.occurred_at DESC
        LIMIT 1
      ) latest_event ON TRUE
      WHERE s.id = ANY($1::uuid[])
        AND ($2::timestamptz IS NULL OR COALESCE(latest_event.occurred_at, s.last_seen_at, s.first_seen_at) >= $2::timestamptz - ($3::int * interval '1 day'))
        AND ($2::timestamptz IS NULL OR COALESCE(latest_event.occurred_at, s.last_seen_at, s.first_seen_at) <= $2::timestamptz)
      ORDER BY touchpoint_occurred_at ASC, s.id ASC
    `,
    [sessionIds, orderOccurredAt, env.ATTRIBUTION_WINDOW_DAYS]
  );

  return result.rows;
}

async function resolveTouchpointChain(client: PoolClient, order: PendingOrder): Promise<AttributionTouchpoint[]> {
  const evidenceBySessionId = new Map<string, SessionEvidence>();

  const registerEvidence = (sessionIds: string[], evidence: SessionEvidence) => {
    for (const sessionId of sessionIds) {
      const existing = evidenceBySessionId.get(sessionId);

      if (!existing || evidence.rank < existing.rank) {
        evidenceBySessionId.set(sessionId, evidence);
      }
    }
  };

  if (order.landing_session_id) {
    registerEvidence([order.landing_session_id], {
      reason: 'matched_by_landing_session_id',
      rank: 1,
      forced: true
    });
  }

  if (order.checkout_token) {
    registerEvidence(
      await fetchSessionIdsByCheckoutToken(client, order.checkout_token, order.order_occurred_at),
      {
        reason: 'matched_by_checkout_token',
        rank: 2,
        forced: true
      }
    );
  }

  if (order.cart_token) {
    registerEvidence(
      await fetchSessionIdsByCartToken(client, order.cart_token, order.order_occurred_at),
      {
        reason: 'matched_by_cart_token',
        rank: 3,
        forced: true
      }
    );
  }

  if (order.customer_identity_id) {
    registerEvidence(
      await fetchIdentitySessionIds(client, order.customer_identity_id, order.order_occurred_at),
      {
        reason: 'matched_by_customer_identity',
        rank: 4,
        forced: false
      }
    );
  }

  const touchpointRows = await fetchTouchpointsBySessionIds(
    client,
    uniqueSessionIds([...evidenceBySessionId.keys()]),
    order.order_occurred_at
  );

  const enrichedTouchpoints = touchpointRows.map<AttributionTouchpoint>((row) => {
    const evidence = evidenceBySessionId.get(row.session_id) ?? {
      reason: 'matched_by_customer_identity',
      rank: 4,
      forced: false
    };

    return {
      sessionId: row.session_id,
      occurredAt: row.touchpoint_occurred_at,
      source: row.attributed_source,
      medium: row.attributed_medium,
      campaign: row.attributed_campaign,
      content: row.attributed_content,
      term: row.attributed_term,
      clickIdType: row.attributed_click_id_type,
      clickIdValue: row.attributed_click_id_value,
      attributionReason: evidence.reason,
      isDirect: isDirectTouchpoint(row),
      isForced: evidence.forced
    };
  });

  if (enrichedTouchpoints.length === 0) {
    return [];
  }

  const hasTaggedNonDirectTouchpoint = enrichedTouchpoints.some((touchpoint) => !touchpoint.isDirect);

  return enrichedTouchpoints.filter((touchpoint) => {
    if (touchpoint.isForced) {
      return true;
    }

    if (!hasTaggedNonDirectTouchpoint) {
      return true;
    }

    return !touchpoint.isDirect;
  });
}

function deriveLegacyAttributionResult(
  attributionCredits: AttributionCredit[],
  fallbackReason = 'unattributed'
): LegacyAttributionResult {
  const primaryCredit = attributionCredits.find((credit) => credit.isPrimary) ?? attributionCredits[0];

  if (!primaryCredit || primaryCredit.sessionId === null) {
    return {
      sessionId: null,
      source: null,
      medium: null,
      campaign: null,
      content: null,
      term: null,
      clickIdType: null,
      clickIdValue: null,
      confidenceScore: '0.00',
      reason: fallbackReason
    };
  }

  return {
    sessionId: primaryCredit.sessionId,
    source: primaryCredit.source,
    medium: primaryCredit.medium,
    campaign: primaryCredit.campaign,
    content: primaryCredit.content,
    term: primaryCredit.term,
    clickIdType: primaryCredit.clickIdType,
    clickIdValue: primaryCredit.clickIdValue,
    confidenceScore: confidenceForReason(primaryCredit.attributionReason),
    reason: primaryCredit.attributionReason
  };
}

async function persistAttributionCredits(
  client: PoolClient,
  shopifyOrderId: string,
  outputs: Record<AttributionModel, AttributionCredit[]>
): Promise<void> {
  await client.query('DELETE FROM attribution_order_credits WHERE shopify_order_id = $1', [shopifyOrderId]);

  for (const attributionModel of ATTRIBUTION_MODELS) {
    for (const credit of outputs[attributionModel]) {
      await client.query(
        `
          INSERT INTO attribution_order_credits (
            shopify_order_id,
            attribution_model,
            touchpoint_position,
            session_id,
            touchpoint_occurred_at,
            attributed_source,
            attributed_medium,
            attributed_campaign,
            attributed_content,
            attributed_term,
            attributed_click_id_type,
            attributed_click_id_value,
            credit_weight,
            revenue_credit,
            is_primary,
            attribution_reason
          )
          VALUES (
            $1,
            $2,
            $3,
            $4::uuid,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16
          )
        `,
        [
          shopifyOrderId,
          credit.attributionModel,
          credit.touchpointPosition,
          credit.sessionId,
          credit.touchpointOccurredAt,
          credit.source,
          credit.medium,
          credit.campaign,
          credit.content,
          credit.term,
          credit.clickIdType,
          credit.clickIdValue,
          credit.creditWeight.toFixed(8),
          credit.revenueCredit,
          credit.isPrimary,
          credit.attributionReason
        ]
      );
    }
  }
}

async function upsertLegacyAttributionResult(
  client: PoolClient,
  shopifyOrderId: string,
  creditOutputs: Record<AttributionModel, AttributionCredit[]>
): Promise<void> {
  const primaryLastTouchResult = deriveLegacyAttributionResult(creditOutputs.last_touch);

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
        'last_touch',
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
      shopifyOrderId,
      primaryLastTouchResult.sessionId,
      primaryLastTouchResult.source,
      primaryLastTouchResult.medium,
      primaryLastTouchResult.campaign,
      primaryLastTouchResult.content,
      primaryLastTouchResult.term,
      primaryLastTouchResult.clickIdType,
      primaryLastTouchResult.clickIdValue,
      primaryLastTouchResult.confidenceScore,
      primaryLastTouchResult.reason
    ]
  );
}

async function refreshDailyAttributionCampaignMetrics(client: PoolClient, metricDates: string[]): Promise<void> {
  if (metricDates.length === 0) {
    return;
  }

  await client.query(
    'DELETE FROM daily_attribution_campaign_metrics WHERE metric_date = ANY($1::date[])',
    [metricDates]
  );

  await client.query(
    `
      WITH attribution_models AS (
        SELECT unnest($2::text[]) AS attribution_model
      ),
      visit_rows AS (
        SELECT
          DATE(s.first_seen_at) AS metric_date,
          m.attribution_model,
          COALESCE(s.initial_utm_source, 'unknown') AS source,
          COALESCE(s.initial_utm_medium, 'unknown') AS medium,
          COALESCE(s.initial_utm_campaign, 'unknown') AS campaign,
          COALESCE(s.initial_utm_content, '') AS content,
          COUNT(*)::int AS visits,
          0::numeric(12, 8) AS orders,
          0::numeric(12, 2) AS revenue
        FROM tracking_sessions s
        CROSS JOIN attribution_models m
        WHERE DATE(s.first_seen_at) = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5, 6
      ),
      order_rows AS (
        SELECT
          DATE(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) AS metric_date,
          c.attribution_model,
          COALESCE(c.attributed_source, 'unknown') AS source,
          COALESCE(c.attributed_medium, 'unknown') AS medium,
          COALESCE(c.attributed_campaign, 'unknown') AS campaign,
          COALESCE(c.attributed_content, '') AS content,
          0::int AS visits,
          COALESCE(SUM(c.credit_weight), 0)::numeric(12, 8) AS orders,
          COALESCE(SUM(c.revenue_credit), 0)::numeric(12, 2) AS revenue
        FROM attribution_order_credits c
        INNER JOIN shopify_orders o ON o.shopify_order_id = c.shopify_order_id
        WHERE DATE(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5, 6
      )
      INSERT INTO daily_attribution_campaign_metrics (
        metric_date,
        attribution_model,
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
        attribution_model,
        source,
        medium,
        campaign,
        content,
        SUM(visits)::int AS visits,
        SUM(orders)::numeric(12, 8) AS orders,
        SUM(revenue)::numeric(12, 2) AS revenue,
        now()
      FROM (
        SELECT * FROM visit_rows
        UNION ALL
        SELECT * FROM order_rows
      ) combined
      GROUP BY 1, 2, 3, 4, 5, 6
    `,
    [metricDates, ATTRIBUTION_MODELS]
  );
}

export async function processPendingAttribution(limit = 100): Promise<number> {
  const pendingOrders = await query<PendingOrder>(
    `
      SELECT
        o.shopify_order_id,
        COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at,
        o.created_at_shopify,
        o.landing_session_id,
        o.checkout_token,
        o.cart_token,
        o.customer_identity_id,
        o.total_price::text
      FROM shopify_orders o
      WHERE NOT EXISTS (
        SELECT 1
        FROM attribution_order_credits c
        WHERE c.shopify_order_id = o.shopify_order_id
      )
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
      const orderOccurredAt = order.order_occurred_at ?? new Date();
      const touchpoints = await resolveTouchpointChain(client, order);
      const outputs = computeAttributionOutputs(touchpoints, {
        orderOccurredAt,
        orderRevenue: order.total_price
      });

      await persistAttributionCredits(client, order.shopify_order_id, outputs);
      await upsertLegacyAttributionResult(client, order.shopify_order_id, outputs);

      metricDates.add(orderOccurredAt.toISOString().slice(0, 10));
    }

    await refreshDailyAttributionCampaignMetrics(client, [...metricDates]);
  });

  return pendingOrders.rowCount;
}
