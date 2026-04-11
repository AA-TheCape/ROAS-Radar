import { randomUUID } from 'node:crypto';

import { type PoolClient } from 'pg';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { buildCanonicalTouchpointDimensions } from '../marketing-dimensions/index.js';
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

type AttributionJobRow = {
  id: number;
  queue_key: string;
  shopify_order_id: string;
  requested_reason: string;
  requested_model_version: number;
  attempts: number;
};

export type AttributionQueueProcessOptions = {
  limit?: number;
  workerId?: string;
  staleScanLimit?: number;
  emitMetrics?: boolean;
};

export type AttributionQueueProcessResult = {
  workerId: string;
  modelVersion: number;
  staleJobsEnqueued: number;
  claimedJobs: number;
  succeededJobs: number;
  failedJobs: number;
  durationMs: number;
};

const DEFAULT_ATTRIBUTION_JOB_REASON = 'order_recompute_requested';

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

function computeRetryDelaySeconds(attempts: number): number {
  const safeAttempts = Math.max(attempts, 1);
  return Math.min(30 * 2 ** (safeAttempts - 1), 30 * 60);
}

function buildQueueKey(shopifyOrderId: string): string {
  return `order:${shopifyOrderId}`;
}

function buildProcessingMetricsLog(result: AttributionQueueProcessResult): string {
  return JSON.stringify({
    event: 'attribution_queue_run',
    workerId: result.workerId,
    modelVersion: result.modelVersion,
    staleJobsEnqueued: result.staleJobsEnqueued,
    claimedJobs: result.claimedJobs,
    succeededJobs: result.succeededJobs,
    failedJobs: result.failedJobs,
    durationMs: result.durationMs
  });
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
    const canonicalDimensions = buildCanonicalTouchpointDimensions({
      source: row.attributed_source,
      medium: row.attributed_medium,
      campaign: row.attributed_campaign,
      content: row.attributed_content,
      term: row.attributed_term,
      clickIdType: row.attributed_click_id_type,
      clickIdValue: row.attributed_click_id_value
    });

    return {
      sessionId: row.session_id,
      occurredAt: row.touchpoint_occurred_at,
      source: canonicalDimensions.source,
      medium: canonicalDimensions.medium,
      campaign: canonicalDimensions.campaign,
      content: canonicalDimensions.content,
      term: canonicalDimensions.term,
      clickIdType: canonicalDimensions.clickIdType,
      clickIdValue: canonicalDimensions.clickIdValue,
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
  const canonicalDimensions = primaryCredit
    ? buildCanonicalTouchpointDimensions({
        source: primaryCredit.source,
        medium: primaryCredit.medium,
        campaign: primaryCredit.campaign,
        content: primaryCredit.content,
        term: primaryCredit.term,
        clickIdType: primaryCredit.clickIdType,
        clickIdValue: primaryCredit.clickIdValue
      })
    : null;

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
    source: canonicalDimensions?.source ?? null,
    medium: canonicalDimensions?.medium ?? null,
    campaign: canonicalDimensions?.campaign ?? null,
    content: canonicalDimensions?.content ?? null,
    term: canonicalDimensions?.term ?? null,
    clickIdType: canonicalDimensions?.clickIdType ?? null,
    clickIdValue: canonicalDimensions?.clickIdValue ?? null,
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
      const canonicalDimensions = buildCanonicalTouchpointDimensions({
        source: credit.source,
        medium: credit.medium,
        campaign: credit.campaign,
        content: credit.content,
        term: credit.term,
        clickIdType: credit.clickIdType,
        clickIdValue: credit.clickIdValue
      });

      await client.query(
        `
          INSERT INTO attribution_order_credits (
            shopify_order_id,
            attribution_model,
            model_version,
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
            $4,
            $5::uuid,
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
            $16,
            $17
          )
        `,
        [
          shopifyOrderId,
          credit.attributionModel,
          env.ATTRIBUTION_MODEL_VERSION,
          credit.touchpointPosition,
          credit.sessionId,
          credit.touchpointOccurredAt,
          canonicalDimensions.source,
          canonicalDimensions.medium,
          canonicalDimensions.campaign,
          canonicalDimensions.content,
          canonicalDimensions.term,
          canonicalDimensions.clickIdType,
          canonicalDimensions.clickIdValue,
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
        model_version,
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
        $12,
        now(),
        1
      )
      ON CONFLICT (shopify_order_id)
      DO UPDATE SET
        session_id = EXCLUDED.session_id,
        attribution_model = EXCLUDED.attribution_model,
        model_version = EXCLUDED.model_version,
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
      env.ATTRIBUTION_MODEL_VERSION,
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

async function enqueueAttributionJob(
  client: PoolClient,
  shopifyOrderId: string,
  requestedReason: string,
  requestedModelVersion = env.ATTRIBUTION_MODEL_VERSION
): Promise<void> {
  await client.query(
    `
      INSERT INTO attribution_jobs (
        queue_key,
        job_type,
        shopify_order_id,
        requested_reason,
        requested_model_version,
        status,
        attempts,
        available_at,
        locked_at,
        locked_by,
        last_error,
        completed_at,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        'order',
        $2,
        $3,
        $4,
        'pending',
        0,
        now(),
        NULL,
        NULL,
        NULL,
        NULL,
        now(),
        now()
      )
      ON CONFLICT (queue_key)
      DO UPDATE SET
        requested_reason = EXCLUDED.requested_reason,
        requested_model_version = GREATEST(
          attribution_jobs.requested_model_version,
          EXCLUDED.requested_model_version
        ),
        available_at = CASE
          WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.available_at
          ELSE now()
        END,
        status = CASE
          WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.status
          ELSE 'pending'
        END,
        locked_at = CASE
          WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.locked_at
          ELSE NULL
        END,
        locked_by = CASE
          WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.locked_by
          ELSE NULL
        END,
        last_error = NULL,
        completed_at = CASE
          WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.completed_at
          ELSE NULL
        END,
        updated_at = now()
    `,
    [buildQueueKey(shopifyOrderId), shopifyOrderId, requestedReason, requestedModelVersion]
  );
}

async function fetchImpactedOrderIdsForTouchpoint(
  client: PoolClient,
  input: {
    sessionId: string;
    shopifyCheckoutToken: string | null;
    shopifyCartToken: string | null;
  }
): Promise<string[]> {
  const sessionResult = await client.query<{ customer_identity_id: string | null }>(
    `
      SELECT customer_identity_id
      FROM tracking_sessions
      WHERE id = $1::uuid
      LIMIT 1
    `,
    [input.sessionId]
  );

  const customerIdentityId = sessionResult.rows[0]?.customer_identity_id ?? null;

  const result = await client.query<{ shopify_order_id: string }>(
    `
      SELECT DISTINCT shopify_order_id
      FROM shopify_orders
      WHERE landing_session_id = $1::uuid
         OR ($2::text IS NOT NULL AND checkout_token = $2)
         OR ($3::text IS NOT NULL AND cart_token = $3)
         OR ($4::uuid IS NOT NULL AND customer_identity_id = $4::uuid)
      ORDER BY shopify_order_id ASC
    `,
    [input.sessionId, input.shopifyCheckoutToken, input.shopifyCartToken, customerIdentityId]
  );

  return result.rows.map((row) => row.shopify_order_id);
}

async function fetchPendingOrder(client: PoolClient, shopifyOrderId: string): Promise<PendingOrder | null> {
  const result = await client.query<PendingOrder>(
    `
      SELECT
        o.shopify_order_id,
        COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at,
        o.landing_session_id,
        o.checkout_token,
        o.cart_token,
        o.customer_identity_id,
        o.total_price::text
      FROM shopify_orders o
      WHERE o.shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  return result.rows[0] ?? null;
}

async function completeAttributionJob(client: PoolClient, jobId: number): Promise<void> {
  await client.query(
    `
      UPDATE attribution_jobs
      SET
        status = 'completed',
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        completed_at = now(),
        updated_at = now()
      WHERE id = $1
    `,
    [jobId]
  );
}

async function retryAttributionJob(client: PoolClient, job: AttributionJobRow, error: unknown): Promise<void> {
  const delaySeconds = computeRetryDelaySeconds(job.attempts);
  const boundedAttempts = Math.max(job.attempts, 1);
  const nextStatus = boundedAttempts >= env.ATTRIBUTION_JOB_MAX_RETRIES ? 'completed' : 'retry';
  const serializedError = error instanceof Error ? error.stack ?? error.message : String(error);

  await client.query(
    `
      UPDATE attribution_jobs
      SET
        status = $2,
        available_at = CASE
          WHEN $2 = 'retry' THEN now() + ($3::int * interval '1 second')
          ELSE now()
        END,
        locked_at = NULL,
        locked_by = NULL,
        last_error = $4,
        completed_at = CASE WHEN $2 = 'completed' THEN now() ELSE NULL END,
        updated_at = now()
      WHERE id = $1
    `,
    [job.id, nextStatus, delaySeconds, serializedError.slice(0, 4000)]
  );
}

async function claimAttributionJobs(limit: number, workerId: string): Promise<AttributionJobRow[]> {
  return withTransaction(async (client) => {
    const result = await client.query<AttributionJobRow>(
      `
        WITH candidate_jobs AS (
          SELECT id
          FROM attribution_jobs
          WHERE job_type = 'order'
            AND (
              (status IN ('pending', 'retry') AND available_at <= now())
              OR (
                status = 'processing'
                AND locked_at <= now() - ($2::int * interval '1 second')
              )
            )
          ORDER BY available_at ASC, id ASC
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE attribution_jobs jobs
        SET
          status = 'processing',
          attempts = jobs.attempts + 1,
          locked_at = now(),
          locked_by = $3,
          updated_at = now()
        FROM candidate_jobs
        WHERE jobs.id = candidate_jobs.id
        RETURNING
          jobs.id,
          jobs.queue_key,
          jobs.shopify_order_id,
          jobs.requested_reason,
          jobs.requested_model_version,
          jobs.attempts
      `,
      [limit, env.ATTRIBUTION_JOB_LEASE_SECONDS, workerId]
    );

    return result.rows;
  });
}

async function processAttributionJob(job: AttributionJobRow): Promise<void> {
  await withTransaction(async (client) => {
    const order = await fetchPendingOrder(client, job.shopify_order_id);

    if (!order) {
      await completeAttributionJob(client, job.id);
      return;
    }

    const orderOccurredAt = order.order_occurred_at ?? new Date();
    const touchpoints = await resolveTouchpointChain(client, order);
    const outputs = computeAttributionOutputs(touchpoints, {
      orderOccurredAt,
      orderRevenue: order.total_price
    });

    await persistAttributionCredits(client, order.shopify_order_id, outputs);
    await upsertLegacyAttributionResult(client, order.shopify_order_id, outputs);
    await refreshDailyAttributionCampaignMetrics(client, [orderOccurredAt.toISOString().slice(0, 10)]);
    await completeAttributionJob(client, job.id);
  });
}

export async function enqueueAttributionForOrder(
  shopifyOrderId: string,
  requestedReason = DEFAULT_ATTRIBUTION_JOB_REASON,
  client?: PoolClient
): Promise<void> {
  if (client) {
    await enqueueAttributionJob(client, shopifyOrderId, requestedReason);
    return;
  }

  await withTransaction(async (transactionClient) => {
    await enqueueAttributionJob(transactionClient, shopifyOrderId, requestedReason);
  });
}

export async function enqueueAttributionForTrackingTouchpoint(
  client: PoolClient,
  input: {
    sessionId: string;
    shopifyCheckoutToken: string | null;
    shopifyCartToken: string | null;
  }
): Promise<number> {
  const orderIds = await fetchImpactedOrderIdsForTouchpoint(client, input);

  for (const shopifyOrderId of orderIds) {
    await enqueueAttributionJob(client, shopifyOrderId, 'tracking_touchpoint_updated');
  }

  return orderIds.length;
}

export async function enqueueStaleAttributionJobs(limit = env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE): Promise<number> {
  const result = await query<{ inserted_count: string }>(
    `
      WITH stale_orders AS (
        SELECT o.shopify_order_id
        FROM shopify_orders o
        LEFT JOIN attribution_results ar ON ar.shopify_order_id = o.shopify_order_id
        WHERE ar.shopify_order_id IS NULL
           OR ar.model_version < $1
        ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) ASC, o.shopify_order_id ASC
        LIMIT $2
      ),
      queued AS (
        INSERT INTO attribution_jobs (
          queue_key,
          job_type,
          shopify_order_id,
          requested_reason,
          requested_model_version,
          status,
          attempts,
          available_at,
          locked_at,
          locked_by,
          last_error,
          completed_at,
          created_at,
          updated_at
        )
        SELECT
          'order:' || shopify_order_id,
          'order',
          shopify_order_id,
          'model_version_changed',
          $1,
          'pending',
          0,
          now(),
          NULL,
          NULL,
          NULL,
          NULL,
          now(),
          now()
        FROM stale_orders
        ON CONFLICT (queue_key)
        DO UPDATE SET
          requested_reason = EXCLUDED.requested_reason,
          requested_model_version = GREATEST(
            attribution_jobs.requested_model_version,
            EXCLUDED.requested_model_version
          ),
          available_at = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.available_at
            ELSE now()
          END,
          status = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.status
            ELSE 'pending'
          END,
          locked_at = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.locked_at
            ELSE NULL
          END,
          locked_by = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.locked_by
            ELSE NULL
          END,
          last_error = NULL,
          completed_at = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.completed_at
            ELSE NULL
          END,
          updated_at = now()
        RETURNING 1
      )
      SELECT COUNT(*)::text AS inserted_count
      FROM queued
    `,
    [env.ATTRIBUTION_MODEL_VERSION, limit]
  );

  return Number(result.rows[0]?.inserted_count ?? 0);
}

export async function processAttributionQueue(
  options: AttributionQueueProcessOptions = {}
): Promise<AttributionQueueProcessResult> {
  const startedAt = Date.now();
  const workerId = options.workerId ?? `worker-${randomUUID()}`;
  const staleJobsEnqueued = await enqueueStaleAttributionJobs(options.staleScanLimit ?? env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE);
  const jobs = await claimAttributionJobs(options.limit ?? env.ATTRIBUTION_JOB_BATCH_SIZE, workerId);

  let succeededJobs = 0;
  let failedJobs = 0;

  for (const job of jobs) {
    try {
      await processAttributionJob(job);
      succeededJobs += 1;
    } catch (error) {
      failedJobs += 1;

      await withTransaction(async (client) => {
        await retryAttributionJob(client, job, error);
      });
    }
  }

  const result: AttributionQueueProcessResult = {
    workerId,
    modelVersion: env.ATTRIBUTION_MODEL_VERSION,
    staleJobsEnqueued,
    claimedJobs: jobs.length,
    succeededJobs,
    failedJobs,
    durationMs: Date.now() - startedAt
  };

  if (options.emitMetrics ?? true) {
    process.stdout.write(`${buildProcessingMetricsLog(result)}\n`);
  }

  return result;
}

export async function processPendingAttribution(limit = env.ATTRIBUTION_JOB_BATCH_SIZE): Promise<number> {
  const result = await processAttributionQueue({
    limit,
    staleScanLimit: limit,
    emitMetrics: false
  });

  return result.succeededJobs;
}

export const __attributionTestUtils = {
  buildProcessingMetricsLog,
  buildQueueKey,
  computeRetryDelaySeconds
};
