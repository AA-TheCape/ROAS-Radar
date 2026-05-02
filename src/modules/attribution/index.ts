import type { PoolClient } from 'pg';

import { withTransaction } from '../../db/pool.js';
import { emitAttributionResolverOutcomeLog, logError } from '../../observability/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { formatDateInTimezone, getReportingTimezone } from '../settings/index.js';
import { collectDeterministicFirstPartyCandidates, extractAttributionCandidatesForOrder } from './candidate-extraction.js';
import {
  ATTRIBUTION_MODELS,
  computeAttributionOutputs,
  computeSingleWinnerCredits,
  type AttributionCredit
} from './engine.js';
import {
  buildAttributionConfidenceLabel,
  buildAttributionMatchSource,
  buildOrderAttributionAuditRecord
} from './order-attribution-audit.js';
import { insertAttributionDecisionArtifact } from './decision-artifacts.js';
import {
  confidenceScoreForWinner,
  dedupeDeterministicCandidates,
  isDirectTouchpoint,
  resolveAttributionTier,
  resolveAttributionTierForVersion,
  selectLastNonDirectWinner,
  type ResolvedJourney,
  type ResolvedAttributionTier,
  type ResolvedAttributionTouchpoint,
  type TieredAttributionResolverInput
} from './resolver.js';
import {
  ATTRIBUTION_RESOLVER_RULE_VERSION,
  selectResolverRuleVersionForForwardProcessing
} from './rule-version.js';

const ATTRIBUTION_MODEL_VERSION = 1;
const JOB_STALE_AFTER_MINUTES = 15;
const MAX_RETRY_DELAY_SECONDS = 1_800;

type OrderRow = {
  shopify_order_id: string;
  total_price: string;
  processed_at: Date | null;
  created_at_shopify: Date | null;
  ingested_at: Date;
  payload_hash: string | null;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  email_hash: string | null;
  customer_identity_id: string | null;
  source_name: string | null;
  attribution_tier: ResolvedAttributionTier | null;
  attribution_resolver_rule_version: string | null;
  raw_payload: unknown;
};

type ClaimedAttributionJob = {
  id: number;
  shopify_order_id: string;
  attempts: number;
  requested_reason: string;
};

type ResolvedAttributionEvaluation = {
  resolverInput: TieredAttributionResolverInput;
  journey: ResolvedJourney;
};

type QueueTouchpointInput = {
  sessionId: string;
  shopifyCheckoutToken?: string | null;
  shopifyCartToken?: string | null;
};

type SyntheticAttributionInput = {
  occurredAt?: Date | null;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  confidenceScore?: number | null;
};

type ProcessAttributionQueueOptions = {
  workerId: string;
  limit: number;
  staleScanLimit?: number;
  emitMetrics?: boolean;
};

type ProcessAttributionQueueResult = {
  workerId: string;
  modelVersion: number;
  staleJobsEnqueued: number;
  claimedJobs: number;
  succeededJobs: number;
  failedJobs: number;
  durationMs: number;
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

export function buildProcessingMetricsLog(result: ProcessAttributionQueueResult): string {
  return JSON.stringify({
    severity: 'INFO',
    event: 'attribution_queue_run',
    message: 'attribution_queue_run',
    timestamp: new Date().toISOString(),
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    ...result
  });
}

function buildQueueOutcomeLog(workerId: string, outcome: string, value: number): string {
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
  input: QueueTouchpointInput
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
    [input.sessionId, input.shopifyCheckoutToken ?? null, input.shopifyCartToken ?? null]
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

  const result = await client.query(
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

async function claimJobs(client: PoolClient, workerId: string, limit: number): Promise<ClaimedAttributionJob[]> {
  const result = await client.query<ClaimedAttributionJob>(
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
      RETURNING j.id, j.shopify_order_id, j.attempts, j.requested_reason
    `,
    [workerId, Math.max(limit, 0)]
  );

  return result.rows;
}

async function fetchOrder(client: PoolClient, shopifyOrderId: string): Promise<OrderRow | null> {
  const result = await client.query<OrderRow>(
    `
      SELECT
        shopify_order_id,
        total_price::text,
        processed_at,
        created_at_shopify,
        ingested_at,
        payload_hash,
        landing_session_id::text,
        checkout_token,
        cart_token,
        email_hash,
        customer_identity_id::text,
        source_name,
        attribution_tier,
        attribution_resolver_rule_version,
        raw_payload
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  return result.rows[0] ?? null;
}

function resolveOrderOccurredAt(order: Pick<OrderRow, 'processed_at' | 'created_at_shopify' | 'ingested_at'>): Date {
  return order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
}

function serializeResolvedTouchpoint(touchpoint: ResolvedAttributionTouchpoint) {
  return {
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
  };
}

function isSameResolvedTouchpoint(
  left: ResolvedAttributionTouchpoint,
  right: ResolvedAttributionTouchpoint
): boolean {
  return (
    left.sessionId === right.sessionId &&
    left.sourceTouchEventId === right.sourceTouchEventId &&
    left.ingestionSource === right.ingestionSource &&
    left.occurredAt.getTime() === right.occurredAt.getTime()
  );
}

async function resolveAttributionJourney(client: PoolClient, order: OrderRow): Promise<ResolvedAttributionEvaluation> {
  const resolverInput = await extractAttributionCandidatesForOrder(client, {
    shopifyOrderId: order.shopify_order_id,
    processedAt: order.processed_at,
    createdAtShopify: order.created_at_shopify,
    ingestedAt: order.ingested_at,
    landingSessionId: order.landing_session_id,
    checkoutToken: order.checkout_token,
    cartToken: order.cart_token,
    emailHash: order.email_hash,
    customerIdentityId: order.customer_identity_id,
    sourceName: order.source_name,
    rawPayload: order.raw_payload
  });

  return {
    resolverInput,
    journey: resolveAttributionTierForVersion(
      resolverInput,
      selectResolverRuleVersionForForwardProcessing({
        attributionTier: order.attribution_tier,
        attributionResolverRuleVersion: order.attribution_resolver_rule_version
      })
    )
  };
}

function selectPrimaryCredit(credits: AttributionCredit[]): AttributionCredit | undefined {
  return credits.find((credit) => credit.isPrimary) ?? credits[credits.length - 1];
}

async function persistAttribution(
  client: PoolClient,
  order: OrderRow,
  evaluation: ResolvedAttributionEvaluation,
  context: {
    resolverRunSource: 'forward_processing' | 'manual_backfill';
    resolverTriggeredBy: string;
    backfillRunId?: string | null;
  }
): Promise<void> {
  const { journey, resolverInput } = evaluation;
  const orderOccurredAt = journey.orderOccurredAtUtc ?? resolveOrderOccurredAt(order);
  const outputs = computeAttributionOutputs(journey.touchpoints, {
    orderOccurredAt,
    orderRevenue: order.total_price
  });

  if (journey.winner) {
    const winner = journey.winner;
    const winnerIndex = journey.touchpoints.findIndex((touchpoint) => isSameResolvedTouchpoint(touchpoint, winner));

    if (winnerIndex >= 0) {
      outputs.last_touch = computeSingleWinnerCredits('last_touch', journey.touchpoints, winnerIndex, order.total_price);
    }
  }

  const primaryCredit = selectPrimaryCredit(outputs.last_touch);

  if (!primaryCredit) {
    throw new Error(`Failed to compute attribution credits for Shopify order ${order.shopify_order_id}`);
  }

  const matchedAt = new Date();
  const orderAttributionAudit = buildOrderAttributionAuditRecord(journey, matchedAt);
  const matchSource = buildAttributionMatchSource(journey);
  const confidenceLabel = buildAttributionConfidenceLabel(journey.confidenceScore);
  const decisionArtifactId = await insertAttributionDecisionArtifact({
    client,
    order: {
      shopifyOrderId: order.shopify_order_id,
      payloadHash: order.payload_hash,
      attributionTier: order.attribution_tier
    },
    journey,
    resolverInput,
    orderAttributionAudit,
    resolverRunSource: context.resolverRunSource,
    resolverTriggeredBy: context.resolverTriggeredBy,
    backfillRunId: context.backfillRunId
  });

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
            model_version,
            match_source,
            confidence_label
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
            $17,
            $18,
            $19
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
          ATTRIBUTION_MODEL_VERSION,
          matchSource,
          confidenceLabel
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
        model_version,
        match_source,
        confidence_label,
        meta_attribution_evaluation_outcome,
        meta_attribution_affected_canonical,
        attribution_decision_artifact_id,
        resolver_rule_version
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
        1,
        $13,
        $14,
        $15,
        $16,
        $17,
        $18::uuid,
        $19
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
        attributed_at = EXCLUDED.attributed_at,
        model_version = EXCLUDED.model_version,
        match_source = EXCLUDED.match_source,
        confidence_label = EXCLUDED.confidence_label,
        meta_attribution_evaluation_outcome = EXCLUDED.meta_attribution_evaluation_outcome,
        meta_attribution_affected_canonical = EXCLUDED.meta_attribution_affected_canonical,
        attribution_decision_artifact_id = EXCLUDED.attribution_decision_artifact_id,
        resolver_rule_version = EXCLUDED.resolver_rule_version
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
      matchedAt,
      ATTRIBUTION_MODEL_VERSION,
      matchSource,
      confidenceLabel,
      'not_evaluated',
      false,
      decisionArtifactId,
      journey.resolverRuleVersion
    ]
  );

  await client.query(
    `
      UPDATE shopify_orders
      SET
        attribution_tier = $2,
        attribution_source = $3,
        attribution_matched_at = $4,
        attribution_reason = $5,
        attribution_snapshot = $6::jsonb,
        attribution_snapshot_updated_at = $4,
        attribution_resolver_rule_version = $7,
        meta_attribution_evaluation_outcome = 'not_evaluated',
        meta_attribution_present = false,
        meta_attribution_affected_canonical = false,
        latest_attribution_decision_artifact_id = $8::uuid
      WHERE shopify_order_id = $1
    `,
    [
      order.shopify_order_id,
      orderAttributionAudit.tier,
      orderAttributionAudit.source,
      orderAttributionAudit.matchedAt,
      orderAttributionAudit.reason,
      JSON.stringify({
        tier: journey.tier,
        resolverRuleVersion: journey.resolverRuleVersion,
        decisionArtifactId,
        attributionReason: journey.attributionReason,
        orderOccurredAtUtc: journey.orderOccurredAtUtc?.toISOString() ?? null,
        normalizationFailures: journey.normalizationFailures,
        confidenceScore: journey.confidenceScore,
        winner: journey.winner ? serializeResolvedTouchpoint(journey.winner) : null,
        timeline: journey.touchpoints.map(serializeResolvedTouchpoint)
      }),
      journey.resolverRuleVersion,
      decisionArtifactId
    ]
  );

  emitAttributionResolverOutcomeLog({
    shopifyOrderId: order.shopify_order_id,
    orderOccurredAtUtc: journey.orderOccurredAtUtc,
    tier: journey.tier,
    attributionReason: journey.attributionReason,
    confidenceScore: journey.confidenceScore,
    resolverRuleVersion: journey.resolverRuleVersion,
    decisionArtifactId,
    pipeline: 'realtime_queue',
    touchpoints: journey.touchpoints,
    winner: journey.winner,
    normalizationFailures: journey.normalizationFailures
  });
}

function primaryCreditReason(journey: ResolvedJourney): string {
  return journey.winner?.attributionReason ?? 'unattributed';
}

async function processClaimedJob(client: PoolClient, job: ClaimedAttributionJob, workerId: string): Promise<void> {
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

  const evaluation = await resolveAttributionJourney(client, order);

  await persistAttribution(client, order, evaluation, {
    resolverRunSource: 'forward_processing',
    resolverTriggeredBy: `attribution_job:${job.requested_reason}`
  });

  const metricDate = formatDateInTimezone(resolveOrderOccurredAt(order), await getReportingTimezone(client));
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
      confidenceScore: evaluation.journey.confidenceScore,
      touchpointCount: evaluation.journey.touchpoints.length,
      attributionReason: primaryCreditReason(evaluation.journey)
    })}\n`
  );
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

    const orderOccurredAt = resolveOrderOccurredAt(order);
    const normalizedSource = normalizeNullableString(input.source);
    const normalizedMedium = normalizeNullableString(input.medium);
    const normalizedCampaign = normalizeNullableString(input.campaign);
    const normalizedContent = normalizeNullableString(input.content);
    const normalizedTerm = normalizeNullableString(input.term);
    const normalizedClickIdType = normalizeNullableString(input.clickIdType);
    const normalizedClickIdValue = normalizeNullableString(input.clickIdValue);
    const touchpoint: ResolvedAttributionTouchpoint = {
      sessionId: null,
      sourceTouchEventId: null,
      occurredAt: input.occurredAt ?? orderOccurredAt,
      source: normalizedSource,
      medium: normalizedMedium,
      campaign: normalizedCampaign,
      content: normalizedContent,
      term: normalizedTerm,
      clickIdType: normalizedClickIdType,
      clickIdValue: normalizedClickIdValue,
      attributionReason: input.attributionReason,
      ingestionSource: 'customer_identity',
      isDirect: isDirectTouchpoint({
        source: normalizedSource,
        medium: normalizedMedium,
        campaign: normalizedCampaign,
        content: normalizedContent,
        term: normalizedTerm,
        clickIdValue: normalizedClickIdValue
      }),
      isForced: true
    };

    await persistAttribution(
      db,
      order,
      {
        resolverInput: {
          orderOccurredAtUtc: orderOccurredAt,
          deterministicFirstParty: [
            {
              sourceKey: `synthetic:${shopifyOrderId}`,
              sessionId: null,
              sourceTouchEventId: null,
              ingestionSource: 'customer_identity',
              occurredAtUtc: touchpoint.occurredAt,
              source: normalizedSource,
              medium: normalizedMedium,
              campaign: normalizedCampaign,
              content: normalizedContent,
              term: normalizedTerm,
              clickIdType: normalizedClickIdType,
              clickIdValue: normalizedClickIdValue,
              attributionReason: input.attributionReason,
              confidenceScore: input.confidenceScore ?? 0.35,
              isDirect: touchpoint.isDirect,
              isSynthetic: true
            }
          ],
          shopifyHint: [],
          ga4Fallback: [],
          normalizationFailures: []
        },
        journey: {
          tier: 'deterministic_first_party',
          touchpoints: [touchpoint],
          winner: touchpoint,
          confidenceScore: input.confidenceScore ?? 0.35,
          attributionReason: input.attributionReason,
          resolverRuleVersion: ATTRIBUTION_RESOLVER_RULE_VERSION,
          orderOccurredAtUtc: orderOccurredAt,
          normalizationFailures: []
        }
      },
      {
        resolverRunSource: 'forward_processing',
        resolverTriggeredBy: 'synthetic_attribution_override'
      }
    );

    const metricDate = formatDateInTimezone(orderOccurredAt, await getReportingTimezone(db));
    await refreshDailyReportingMetrics(db, [metricDate]);
  });
}

async function markJobForRetry(
  client: PoolClient,
  job: ClaimedAttributionJob,
  workerId: string,
  error: unknown
): Promise<void> {
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
  options: ProcessAttributionQueueOptions
): Promise<ProcessAttributionQueueResult> {
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

  const summary: ProcessAttributionQueueResult = {
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
  selectLastNonDirectWinner,
  confidenceScoreForWinner,
  resolveAttributionTier,
  resolveAttributionTierForVersion,
  collectDeterministicFirstPartyCandidates,
  extractAttributionCandidatesForOrder
};
