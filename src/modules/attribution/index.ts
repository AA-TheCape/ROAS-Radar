import type { PoolClient } from 'pg';

import { query, withTransaction } from '../../db/pool.js';
import { logError } from '../../observability/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { getReportingTimezone, formatDateInTimezone } from '../settings/index.js';
import { enqueueShopifyOrderWriteback } from '../shopify/writeback.js';
import {
  ATTRIBUTION_MODELS,
  buildUnattributedTouchpoint,
  computeSingleWinnerCredits,
  computeAttributionOutputs,
  type AttributionCredit
} from './engine.js';
import {
  confidenceScoreForWinner,
  dedupeDeterministicCandidates,
  isDirectTouchpoint,
  selectLastNonDirectWinner,
  type DeterministicIngestionSource,
  type ResolvedAttributionTouchpoint,
  type ResolvedJourney
} from './resolver.js';

const ATTRIBUTION_MODEL_VERSION = 1;
const ATTRIBUTION_WINDOW_DAYS = 7;
const JOB_STALE_AFTER_MINUTES = 15;
const MAX_RETRY_DELAY_SECONDS = 1_800;

type AttributionOrderRow = {
  shopify_order_id: string;
  total_price: string;
  processed_at: Date | null;
  created_at_shopify: Date | null;
  ingested_at: Date;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  email_hash: string | null;
  customer_identity_id: string | null;
};

type AttributionJobRow = {
  id: number;
  shopify_order_id: string;
  attempts: number;
};

type SessionTouchpointRow = {
  session_id: string;
  source_touch_event_id: string | null;
  occurred_at: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  click_id_type: string | null;
  click_id_value: string | null;
};

type TouchpointEnqueueInput = {
  sessionId: string;
  shopifyCheckoutToken: string | null;
  shopifyCartToken: string | null;
};

export type AttributionQueueProcessOptions = {
  workerId: string;
  limit: number;
  staleScanLimit?: number;
  emitMetrics?: boolean;
};

export type AttributionQueueProcessResult = {
  workerId: string;
  modelVersion: string | number;
  staleJobsEnqueued: number;
  claimedJobs: number;
  succeededJobs: number;
  failedJobs: number;
  durationMs: number;
};

export type SyntheticAttributionInput = {
  occurredAt?: Date | null;
  source?: string | null;
  medium?: string | null;
  campaign?: string | null;
  content?: string | null;
  term?: string | null;
  clickIdType?: string | null;
  clickIdValue?: string | null;
  attributionReason: string;
  confidenceScore?: number;
};

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

export function buildQueueKey(shopifyOrderId: string): string {
  return `order:${shopifyOrderId}`;
}

export function computeRetryDelaySeconds(attempts: number): number {
  const normalizedAttempts = Number.isFinite(attempts) ? Math.max(Math.trunc(attempts), 1) : 1;
  return Math.min(30 * 2 ** (normalizedAttempts - 1), MAX_RETRY_DELAY_SECONDS);
}

export function buildProcessingMetricsLog(result: AttributionQueueProcessResult): string {
  return JSON.stringify({
    severity: 'INFO',
    event: 'attribution_queue_run',
    message: 'attribution_queue_run',
    timestamp: new Date().toISOString(),
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    ...result
  });
}

function buildQueueOutcomeLog(workerId: string, outcome: 'claimed' | 'succeeded' | 'failed', value: number): string {
  return JSON.stringify({
    severity: 'INFO',
    event: 'attribution_queue_outcome',
    message: 'attribution_queue_outcome',
    timestamp: new Date().toISOString(),
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    workerId,
    outcome,
    value
  });
}

async function execute<T>(client: PoolClient | undefined, callback: (db: PoolClient) => Promise<T>): Promise<T> {
  if (client) {
    return callback(client);
  }

  return withTransaction(callback);
}

export async function enqueueAttributionForOrder(
  shopifyOrderId: string,
  requestedReason: string,
  client?: PoolClient
): Promise<void> {
  await execute(client, async (db) => {
    await db.query(
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
          updated_at
        )
        VALUES ($1, 'order', $2, $3, $4, 'pending', 0, now(), now())
        ON CONFLICT (queue_key)
        DO UPDATE SET
          requested_reason = EXCLUDED.requested_reason,
          requested_model_version = EXCLUDED.requested_model_version,
          status = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.status
            ELSE 'pending'
          END,
          available_at = CASE
            WHEN attribution_jobs.status = 'processing' THEN attribution_jobs.available_at
            ELSE now()
          END,
          completed_at = NULL,
          last_error = NULL,
          updated_at = now()
      `,
      [buildQueueKey(shopifyOrderId), shopifyOrderId, requestedReason, ATTRIBUTION_MODEL_VERSION]
    );
  });
}

export async function enqueueAttributionForTrackingTouchpoint(
  client: PoolClient,
  input: TouchpointEnqueueInput
): Promise<number> {
  const result = await client.query<{ shopify_order_id: string }>(
    `
      SELECT DISTINCT o.shopify_order_id
      FROM shopify_orders o
      WHERE o.landing_session_id = $1::uuid
         OR ($2::text IS NOT NULL AND o.checkout_token = $2)
         OR ($3::text IS NOT NULL AND o.cart_token = $3)
         OR EXISTS (
           SELECT 1
           FROM tracking_sessions s
           WHERE s.id = $1::uuid
             AND s.customer_identity_id IS NOT NULL
             AND s.customer_identity_id = o.customer_identity_id
         )
    `,
    [input.sessionId, input.shopifyCheckoutToken, input.shopifyCartToken]
  );

  for (const row of result.rows) {
    await enqueueAttributionForOrder(row.shopify_order_id, 'tracking_touchpoint_updated', client);
  }

  return result.rowCount ?? result.rows.length;
}

async function requeueStaleJobs(client: PoolClient, staleScanLimit: number): Promise<number> {
  if (staleScanLimit <= 0) {
    return 0;
  }

  const result = await client.query<{ id: number }>(
    `
      WITH stale_jobs AS (
        SELECT id
        FROM attribution_jobs
        WHERE status = 'processing'
          AND locked_at < now() - ($1::int * interval '1 minute')
        ORDER BY locked_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE attribution_jobs j
      SET
        status = 'retry',
        locked_at = NULL,
        locked_by = NULL,
        available_at = now(),
        last_error = COALESCE(j.last_error, 'job_requeued_after_stale_lock'),
        updated_at = now()
      FROM stale_jobs
      WHERE j.id = stale_jobs.id
      RETURNING j.id
    `,
    [JOB_STALE_AFTER_MINUTES, staleScanLimit]
  );

  return result.rowCount ?? result.rows.length;
}

async function claimJobs(client: PoolClient, workerId: string, limit: number): Promise<AttributionJobRow[]> {
  const result = await client.query<AttributionJobRow>(
    `
      WITH candidate_jobs AS (
        SELECT id
        FROM attribution_jobs
        WHERE status IN ('pending', 'retry')
          AND available_at <= now()
        ORDER BY available_at ASC, id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE attribution_jobs j
      SET
        status = 'processing',
        locked_at = now(),
        locked_by = $1,
        attempts = j.attempts + 1,
        updated_at = now()
      FROM candidate_jobs
      WHERE j.id = candidate_jobs.id
      RETURNING j.id, j.shopify_order_id, j.attempts
    `,
    [workerId, Math.max(limit, 0)]
  );

  return result.rows;
}

async function fetchOrder(client: PoolClient, shopifyOrderId: string): Promise<AttributionOrderRow | null> {
  const result = await client.query<AttributionOrderRow>(
    `
      SELECT
        shopify_order_id,
        total_price::text,
        processed_at,
        created_at_shopify,
        ingested_at,
        landing_session_id::text,
        checkout_token,
        cart_token,
        email_hash,
        customer_identity_id::text
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  return result.rows[0] ?? null;
}

function buildResolvedTouchpoint(
  row: SessionTouchpointRow,
  ingestionSource: DeterministicIngestionSource,
  attributionReason: string,
  isForced: boolean
): ResolvedAttributionTouchpoint {
  return {
    sessionId: row.session_id,
    occurredAt: row.occurred_at,
    source: row.source,
    medium: row.medium,
    campaign: row.campaign,
    content: row.content,
    term: row.term,
    clickIdType: row.click_id_type,
    clickIdValue: row.click_id_value,
    attributionReason,
    isDirect: isDirectTouchpoint({
      source: row.source,
      medium: row.medium,
      campaign: row.campaign,
      content: row.content,
      term: row.term,
      clickIdValue: row.click_id_value
    }),
    isForced,
    sourceTouchEventId: row.source_touch_event_id,
    ingestionSource
  };
}

async function fetchLandingSessionCandidate(
  client: PoolClient,
  sessionId: string,
  orderOccurredAt: Date
): Promise<ResolvedAttributionTouchpoint | null> {
  const result = await client.query<SessionTouchpointRow>(
    `
      SELECT
        s.id::text AS session_id,
        first_event.id::text AS source_touch_event_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT e.id
        FROM tracking_events e
        WHERE e.session_id = s.id
          AND e.occurred_at <= $2
        ORDER BY e.occurred_at ASC, e.id ASC
        LIMIT 1
      ) first_event ON TRUE
      WHERE s.id = $1::uuid
        AND s.first_seen_at <= $2
        AND s.first_seen_at >= $2 - ($3::int * interval '1 day')
      LIMIT 1
    `,
    [sessionId, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]
  );

  const row = result.rows[0];
  return row ? buildResolvedTouchpoint(row, 'landing_session_id', 'matched_by_landing_session', true) : null;
}

async function fetchTokenCandidate(
  client: PoolClient,
  tokenField: 'shopify_checkout_token' | 'shopify_cart_token',
  tokenValue: string,
  orderOccurredAt: Date,
  ingestionSource: 'checkout_token' | 'cart_token',
  attributionReason: string
): Promise<ResolvedAttributionTouchpoint | null> {
  const result = await client.query<SessionTouchpointRow>(
    `
      SELECT
        e.session_id::text AS session_id,
        e.id::text AS source_touch_event_id,
        e.occurred_at,
        COALESCE(e.utm_source, s.initial_utm_source) AS source,
        COALESCE(e.utm_medium, s.initial_utm_medium) AS medium,
        COALESCE(e.utm_campaign, s.initial_utm_campaign) AS campaign,
        COALESCE(e.utm_content, s.initial_utm_content) AS content,
        COALESCE(e.utm_term, s.initial_utm_term) AS term,
        CASE
          WHEN e.gclid IS NOT NULL THEN 'gclid'
          WHEN e.gbraid IS NOT NULL THEN 'gbraid'
          WHEN e.wbraid IS NOT NULL THEN 'wbraid'
          WHEN e.fbclid IS NOT NULL THEN 'fbclid'
          WHEN e.ttclid IS NOT NULL THEN 'ttclid'
          WHEN e.msclkid IS NOT NULL THEN 'msclkid'
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          e.gclid,
          e.gbraid,
          e.wbraid,
          e.fbclid,
          e.ttclid,
          e.msclkid,
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_events e
      INNER JOIN tracking_sessions s
        ON s.id = e.session_id
      WHERE ${tokenField} = $1
        AND e.occurred_at <= $2
        AND e.occurred_at >= $2 - ($3::int * interval '1 day')
      ORDER BY
        e.occurred_at DESC,
        CASE
          WHEN COALESCE(
            e.gclid,
            e.gbraid,
            e.wbraid,
            e.fbclid,
            e.ttclid,
            e.msclkid,
            s.initial_gclid,
            s.initial_gbraid,
            s.initial_wbraid,
            s.initial_fbclid,
            s.initial_ttclid,
            s.initial_msclkid
          ) IS NOT NULL THEN 0
          ELSE 1
        END ASC,
        e.session_id::text ASC,
        e.id::text ASC
      LIMIT 1
    `,
    [tokenValue, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]
  );

  const row = result.rows[0];
  return row ? buildResolvedTouchpoint(row, ingestionSource, attributionReason, true) : null;
}

async function fetchIdentityCandidates(
  client: PoolClient,
  order: AttributionOrderRow,
  orderOccurredAt: Date
): Promise<ResolvedAttributionTouchpoint[]> {
  if (!order.customer_identity_id) {
    return [];
  }

  const result = await client.query<SessionTouchpointRow>(
    `
      SELECT
        s.id::text AS session_id,
        first_event.id::text AS source_touch_event_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT e.id
        FROM tracking_events e
        WHERE e.session_id = s.id
          AND e.occurred_at <= $2
        ORDER BY e.occurred_at ASC, e.id ASC
        LIMIT 1
      ) first_event ON TRUE
      WHERE s.customer_identity_id = $1::uuid
        AND s.first_seen_at <= $2
        AND s.first_seen_at >= $2 - ($3::int * interval '1 day')
      ORDER BY s.first_seen_at ASC, s.id ASC
    `,
    [order.customer_identity_id, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]
  );

  return result.rows.map((row) =>
    buildResolvedTouchpoint(row, 'customer_identity', 'matched_by_customer_identity', false)
  );
}

async function resolveAttributionJourney(client: PoolClient, order: AttributionOrderRow): Promise<ResolvedJourney> {
  const orderOccurredAt = order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
  const candidates: ResolvedAttributionTouchpoint[] = [];

  if (order.landing_session_id) {
    const landingCandidate = await fetchLandingSessionCandidate(client, order.landing_session_id, orderOccurredAt);
    if (landingCandidate) {
      candidates.push(landingCandidate);
    }
  }

  if (order.checkout_token) {
    const checkoutCandidate = await fetchTokenCandidate(
      client,
      'shopify_checkout_token',
      order.checkout_token,
      orderOccurredAt,
      'checkout_token',
      'matched_by_checkout_token'
    );
    if (checkoutCandidate) {
      candidates.push(checkoutCandidate);
    }
  }

  if (order.cart_token) {
    const cartCandidate = await fetchTokenCandidate(
      client,
      'shopify_cart_token',
      order.cart_token,
      orderOccurredAt,
      'cart_token',
      'matched_by_cart_token'
    );
    if (cartCandidate) {
      candidates.push(cartCandidate);
    }
  }

  candidates.push(...(await fetchIdentityCandidates(client, order, orderOccurredAt)));

  const touchpoints = dedupeDeterministicCandidates(candidates);
  const winner = selectLastNonDirectWinner(touchpoints);

  return {
    touchpoints,
    winner,
    confidenceScore: confidenceScoreForWinner(winner)
  };
}

function selectPrimaryCredit(credits: AttributionCredit[]): AttributionCredit {
  return credits.find((credit) => credit.isPrimary) ?? credits[credits.length - 1];
}

function buildOrderAttributionSnapshot(orderOccurredAt: Date, journey: ResolvedJourney): Record<string, unknown> {
  return {
    version: 1,
    resolvedAt: new Date().toISOString(),
    orderOccurredAt: orderOccurredAt.toISOString(),
    confidenceScore: journey.confidenceScore,
    winner: journey.winner
      ? {
          sessionId: journey.winner.sessionId,
          sourceTouchEventId: journey.winner.sourceTouchEventId,
          occurredAt: journey.winner.occurredAt.toISOString(),
          source: journey.winner.source,
          medium: journey.winner.medium,
          campaign: journey.winner.campaign,
          content: journey.winner.content,
          term: journey.winner.term,
          clickIdType: journey.winner.clickIdType,
          clickIdValue: journey.winner.clickIdValue,
          attributionReason: journey.winner.attributionReason,
          ingestionSource: journey.winner.ingestionSource,
          isDirect: journey.winner.isDirect
        }
      : null,
    timeline: journey.touchpoints.map((touchpoint) => ({
      sessionId: touchpoint.sessionId,
      sourceTouchEventId: touchpoint.sourceTouchEventId,
      occurredAt: touchpoint.occurredAt.toISOString(),
      source: touchpoint.source,
      medium: touchpoint.medium,
      campaign: touchpoint.campaign,
      content: touchpoint.content,
      term: touchpoint.term,
      clickIdType: touchpoint.clickIdType,
      clickIdValue: touchpoint.clickIdValue,
      attributionReason: touchpoint.attributionReason,
      ingestionSource: touchpoint.ingestionSource,
      isDirect: touchpoint.isDirect
    }))
  };
}

async function persistAttribution(
  client: PoolClient,
  order: AttributionOrderRow,
  journey: ResolvedJourney
): Promise<void> {
  const orderOccurredAt = order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
  const orderedTouchpoints = journey.touchpoints.length > 0 ? journey.touchpoints : [buildUnattributedTouchpoint(orderOccurredAt)];
  const outputs = computeAttributionOutputs(orderedTouchpoints, {
    orderOccurredAt,
    orderRevenue: order.total_price
  });
  const winnerIndex = journey.winner ? orderedTouchpoints.findIndex((touchpoint) => touchpoint === journey.winner) : -1;

  if (winnerIndex >= 0) {
    outputs.last_touch = computeSingleWinnerCredits('last_touch', orderedTouchpoints, winnerIndex, order.total_price);
  }

  const primaryCredit = selectPrimaryCredit(outputs.last_touch);

  await client.query('DELETE FROM attribution_order_credits WHERE shopify_order_id = $1', [order.shopify_order_id]);

  for (const model of ATTRIBUTION_MODELS) {
    const modelCredits = outputs[model];

    for (const credit of modelCredits) {
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
            attribution_reason,
            model_version
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
            $16,
            $17
          )
        `,
        [
          order.shopify_order_id,
          credit.attributionModel,
          credit.touchpointPosition,
          credit.sessionId,
          credit.touchpointOccurredAt,
          normalizeNullableString(credit.source),
          normalizeNullableString(credit.medium),
          normalizeNullableString(credit.campaign),
          normalizeNullableString(credit.content),
          normalizeNullableString(credit.term),
          normalizeNullableString(credit.clickIdType),
          normalizeNullableString(credit.clickIdValue),
          credit.creditWeight,
          credit.revenueCredit,
          credit.isPrimary,
          credit.attributionReason,
          ATTRIBUTION_MODEL_VERSION
        ]
      );
    }
  }

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
        reprocess_version,
        model_version
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
        1,
        $12
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
        model_version = EXCLUDED.model_version
    `,
    [
      order.shopify_order_id,
      primaryCredit.sessionId,
      normalizeNullableString(primaryCredit.source),
      normalizeNullableString(primaryCredit.medium),
      normalizeNullableString(primaryCredit.campaign),
      normalizeNullableString(primaryCredit.content),
      normalizeNullableString(primaryCredit.term),
      normalizeNullableString(primaryCredit.clickIdType),
      normalizeNullableString(primaryCredit.clickIdValue),
      journey.confidenceScore,
      primaryCredit.attributionReason,
      ATTRIBUTION_MODEL_VERSION
    ]
  );

  await client.query(
    `
      UPDATE shopify_orders
      SET
        attribution_snapshot = $2::jsonb,
        attribution_snapshot_updated_at = now()
      WHERE shopify_order_id = $1
    `,
    [order.shopify_order_id, JSON.stringify(buildOrderAttributionSnapshot(orderOccurredAt, journey))]
  );

  await enqueueShopifyOrderWriteback(order.shopify_order_id, 'attribution_persisted', client);
}

async function processClaimedJob(client: PoolClient, job: AttributionJobRow, workerId: string): Promise<void> {
  const order = await fetchOrder(client, job.shopify_order_id);

  if (!order) {
    await client.query(
      `
        UPDATE attribution_jobs
        SET
          status = 'completed',
          completed_at = now(),
          locked_at = NULL,
          locked_by = NULL,
          last_error = 'order_not_found',
          updated_at = now()
        WHERE id = $1
      `,
      [job.id]
    );
    process.stdout.write(
      `${JSON.stringify({
        severity: 'WARNING',
        event: 'attribution_job_skipped',
        message: 'attribution_job_skipped',
        timestamp: new Date().toISOString(),
        workerId,
        shopifyOrderId: job.shopify_order_id,
        reason: 'order_not_found'
      })}\n`
    );
    return;
  }

  const journey = await resolveAttributionJourney(client, order);
  await persistAttribution(client, order, journey);

  const metricDate = formatDateInTimezone(
    order.processed_at ?? order.created_at_shopify ?? order.ingested_at,
    await getReportingTimezone(client)
  );
  await refreshDailyReportingMetrics(client, [metricDate]);

  await client.query(
    `
      UPDATE attribution_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = now()
      WHERE id = $1
        AND locked_by = $2
    `,
    [job.id, workerId]
  );

  process.stdout.write(
    `${JSON.stringify({
      severity: 'INFO',
      event: 'attribution_job_processed',
      message: 'attribution_job_processed',
      timestamp: new Date().toISOString(),
      workerId,
      shopifyOrderId: job.shopify_order_id,
      confidenceScore: journey.confidenceScore,
      touchpointCount: journey.touchpoints.length,
      attributionReason: primaryCreditReason(journey)
    })}\n`
  );
}

function primaryCreditReason(journey: ResolvedJourney): string {
  return journey.winner?.attributionReason ?? 'unattributed';
}

export async function applySyntheticAttributionForOrder(
  shopifyOrderId: string,
  input: SyntheticAttributionInput,
  client?: PoolClient
): Promise<void> {
  await execute(client, async (db) => {
    const order = await fetchOrder(db, shopifyOrderId);

    if (!order) {
      throw new Error(`Shopify order ${shopifyOrderId} not found`);
    }

    const orderOccurredAt = order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
    const touchpoint: ResolvedAttributionTouchpoint = {
      sessionId: null,
      occurredAt: input.occurredAt ?? orderOccurredAt,
      source: normalizeNullableString(input.source),
      medium: normalizeNullableString(input.medium),
      campaign: normalizeNullableString(input.campaign),
      content: normalizeNullableString(input.content),
      term: normalizeNullableString(input.term),
      clickIdType: normalizeNullableString(input.clickIdType),
      clickIdValue: normalizeNullableString(input.clickIdValue),
      attributionReason: input.attributionReason,
      isDirect: isDirectTouchpoint({
        source: normalizeNullableString(input.source),
        medium: normalizeNullableString(input.medium),
        campaign: normalizeNullableString(input.campaign),
        content: normalizeNullableString(input.content),
        term: normalizeNullableString(input.term),
        clickIdValue: normalizeNullableString(input.clickIdValue)
      }),
      isForced: true,
      sourceTouchEventId: null,
      ingestionSource: 'customer_identity'
    };

    await persistAttribution(db, order, {
      touchpoints: [touchpoint],
      winner: touchpoint,
      confidenceScore: input.confidenceScore ?? 0.35
    });

    const metricDate = formatDateInTimezone(orderOccurredAt, await getReportingTimezone(db));
    await refreshDailyReportingMetrics(db, [metricDate]);
  });
}

async function markJobForRetry(client: PoolClient, job: AttributionJobRow, workerId: string, error: unknown): Promise<void> {
  await client.query(
    `
      UPDATE attribution_jobs
      SET
        status = 'retry',
        available_at = now() + ($3::int * interval '1 second'),
        locked_at = NULL,
        locked_by = NULL,
        last_error = $4,
        updated_at = now()
      WHERE id = $1
        AND locked_by = $2
    `,
    [
      job.id,
      workerId,
      computeRetryDelaySeconds(job.attempts),
      error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000)
    ]
  );
}

export async function processAttributionQueue(
  options: AttributionQueueProcessOptions
): Promise<AttributionQueueProcessResult> {
  const startedAt = Date.now();

  const result = await withTransaction(async (client) => {
    const staleJobsEnqueued = await requeueStaleJobs(client, options.staleScanLimit ?? 0);
    const claimedJobs = await claimJobs(client, options.workerId, options.limit);

    return {
      staleJobsEnqueued,
      claimedJobs
    };
  });

  let succeededJobs = 0;
  let failedJobs = 0;

  for (const job of result.claimedJobs) {
    try {
      await withTransaction(async (client) => {
        await processClaimedJob(client, job, options.workerId);
      });
      succeededJobs += 1;
    } catch (error) {
      failedJobs += 1;
      logError('attribution_job_failed', error, {
        workerId: options.workerId,
        shopifyOrderId: job.shopify_order_id,
        attempts: job.attempts
      });
      await withTransaction(async (client) => {
        await markJobForRetry(client, job, options.workerId, error);
      });
    }
  }

  const summary: AttributionQueueProcessResult = {
    workerId: options.workerId,
    modelVersion: ATTRIBUTION_MODEL_VERSION,
    staleJobsEnqueued: result.staleJobsEnqueued,
    claimedJobs: result.claimedJobs.length,
    succeededJobs,
    failedJobs,
    durationMs: Date.now() - startedAt
  };

  if (options.emitMetrics) {
    process.stdout.write(`${buildProcessingMetricsLog(summary)}\n`);
    process.stdout.write(`${buildQueueOutcomeLog(summary.workerId, 'claimed', summary.claimedJobs)}\n`);
    process.stdout.write(`${buildQueueOutcomeLog(summary.workerId, 'succeeded', summary.succeededJobs)}\n`);
    process.stdout.write(`${buildQueueOutcomeLog(summary.workerId, 'failed', summary.failedJobs)}\n`);
  }

  return summary;
}

export const __attributionTestUtils = {
  buildQueueKey,
  computeRetryDelaySeconds,
  buildProcessingMetricsLog,
  dedupeDeterministicCandidates,
  selectLastNonDirectWinner
};
