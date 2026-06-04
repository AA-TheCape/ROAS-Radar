import { Router } from 'express';
import { z } from 'zod';

import { query } from '../../db/pool.js';
import { replayDeadLetters } from '../dead-letters/index.js';
import {
  campaignResolverRequestSchema,
  resolveCampaignMetadata
} from '../campaign-resolver/index.js';
import type { AuthContext } from '../auth/index.js';
import {
  normalizeOrderAttributionBackfillRequest,
  type OrderAttributionBackfillRequest
} from '../../../packages/attribution-schema/index.js';
import { enqueueOrderAttributionBackfillRun } from './backfill-run-store.js';

class AdminDebugHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'AdminDebugHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

const replayTriggerSchema = z.object({
  eventType: z.string().trim().min(1).max(128).optional(),
  sourceTable: z.string().trim().min(1).max(128).optional(),
  status: z.enum(['pending_replay', 'replayed']).optional(),
  fromTime: z.string().datetime().optional(),
  toTime: z.string().datetime().optional(),
  limit: z.coerce.number().int().positive().max(500).optional(),
  dryRun: z.boolean().optional()
});

const auditQuerySchema = z.object({
  limit: z.coerce.number().int().positive().max(100).optional().default(25),
  action: z.string().trim().min(1).max(128).optional(),
  targetType: z.string().trim().min(1).max(128).optional(),
  targetId: z.string().trim().min(1).max(255).optional()
});

type AdminActor = {
  actorKind: 'internal' | 'user';
  actorUserId: number | null;
  actorEmail: string;
};

type OrderJourneyRow = {
  shopify_order_id: string;
  shopify_order_number: string | null;
  shopify_customer_id: string | null;
  currency_code: string;
  subtotal_price: string | number;
  total_price: string | number;
  processed_at: Date | null;
  created_at_shopify: Date | null;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  identity_journey_id: string | null;
  attribution_model: string | null;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attributed_content: string | null;
  attributed_term: string | null;
  confidence_score: string | number | null;
  attribution_reason: string | null;
  attributed_at: Date | null;
};

type LatestAttributionRunRow = {
  run_id: string;
  run_status: string;
  trigger_source: string;
  created_at_utc: Date;
  completed_at_utc: Date | null;
  order_occurred_at_utc: Date;
  identity_journey_id: string | null;
};

type AttributionTouchpointRow = {
  touchpoint_id: string;
  session_id: string | null;
  identity_journey_id: string | null;
  touchpoint_occurred_at_utc: Date;
  touchpoint_captured_at_utc: Date;
  touchpoint_source_kind: string;
  ingestion_source: string;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  click_id_type: string | null;
  click_id_value: string | null;
  evidence_source: string;
  is_direct: boolean;
  engagement_type: string;
  is_synthetic: boolean;
  is_eligible: boolean;
  ineligibility_reason: string | null;
  attribution_reason: string | null;
  attribution_hint: unknown;
};

type AttributionSummaryRow = {
  model_key: string;
  allocation_status: string;
  winner_touchpoint_id: string | null;
  winner_session_id: string | null;
  winner_evidence_source: string | null;
  winner_attribution_reason: string | null;
  total_credit_weight: string | number;
  total_revenue_credited: string | number;
  touchpoint_count_considered: number;
  eligible_click_count: number;
  eligible_view_count: number;
  lookback_rule_applied: string;
  winner_selection_rule: string;
  direct_suppression_applied: boolean;
  deterministic_block_applied: boolean;
  normalization_failures_count: number;
};

type AttributionCreditRow = {
  model_key: string;
  touchpoint_id: string;
  session_id: string | null;
  touchpoint_position: number;
  occurred_at_utc: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  evidence_source: string;
  attribution_reason: string;
  credit_weight: string | number;
  revenue_credit: string | number;
  is_primary: boolean;
  confidence_label: string;
};

type AttributionExplainRow = {
  touchpoint_id: string | null;
  model_key: string | null;
  explain_stage: string;
  decision: string;
  decision_reason: string;
  details_json: unknown;
  created_at_utc: Date;
};

type TrackingEventRow = {
  source_table: string;
  id: string;
  session_id: string;
  event_type: string;
  occurred_at: Date;
  page_url: string | null;
  referrer_url: string | null;
  utm_source: string | null;
  utm_medium: string | null;
  utm_campaign: string | null;
  gclid: string | null;
  fbclid: string | null;
  shopify_cart_token: string | null;
  shopify_checkout_token: string | null;
  ingestion_source: string | null;
};

type IdentityJourneyRow = {
  id: string;
  authoritative_shopify_customer_id: string | null;
  status: string;
  merge_version: number;
  merged_into_journey_id: string | null;
  primary_email_hash: string | null;
  primary_phone_hash: string | null;
  lookback_window_started_at: Date;
  lookback_window_expires_at: Date;
  last_touch_eligible_at: Date;
  first_source_system: string | null;
  last_source_system: string | null;
  created_at: Date;
  updated_at: Date;
};

type IdentityEdgeRow = {
  edge_id: string;
  node_type: string;
  node_key: string;
  edge_type: string;
  precedence_rank: number;
  evidence_source: string;
  source_table: string | null;
  source_record_id: string | null;
  is_active: boolean;
  conflict_code: string | null;
  first_observed_at: Date;
  last_observed_at: Date;
};

type MergeAuditRow = {
  id: string;
  winner_journey_id: string;
  loser_journey_id: string;
  merge_reason_code: string;
  evidence_source: string;
  source_table: string | null;
  source_record_id: string | null;
  source_timestamp: Date;
  winner_score: unknown;
  loser_score: unknown;
  candidate_scores: unknown;
  rehomed_nodes: number;
  quarantined_nodes: number;
  created_at: Date;
};

type AuditLogRow = {
  id: string;
  actor_kind: 'internal' | 'user';
  actor_user_id: string | number | null;
  actor_email: string;
  action: string;
  target_type: string;
  target_id: string | null;
  request_payload: unknown;
  result_summary: unknown;
  created_at: Date;
};

function parseInput<TSchema extends z.ZodTypeAny>(
  schema: TSchema,
  input: unknown,
  message: string
): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new AdminDebugHttpError(400, 'invalid_request', message, error.flatten());
    }

    throw error;
  }
}

function parseBackfillRequest(input: unknown): OrderAttributionBackfillRequest {
  try {
    return normalizeOrderAttributionBackfillRequest(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new AdminDebugHttpError(
        400,
        'invalid_request',
        'Invalid recompute request',
        error.flatten()
      );
    }

    throw error;
  }
}

function getActor(auth: AuthContext | null | undefined): AdminActor {
  if (!auth) {
    throw new AdminDebugHttpError(401, 'unauthorized', 'Authentication required');
  }

  if (auth.kind === 'internal') {
    return {
      actorKind: 'internal',
      actorUserId: null,
      actorEmail: 'internal@system'
    };
  }

  return {
    actorKind: 'user',
    actorUserId: auth.user.id,
    actorEmail: auth.user.email
  };
}

async function writeAdminDebugAudit(input: {
  actor: AdminActor;
  action: string;
  targetType: string;
  targetId?: string | null;
  requestPayload?: unknown;
  resultSummary?: unknown;
}) {
  await query(
    `
      INSERT INTO admin_debug_audit_log (
        actor_kind,
        actor_user_id,
        actor_email,
        action,
        target_type,
        target_id,
        request_payload,
        result_summary
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)
    `,
    [
      input.actor.actorKind,
      input.actor.actorUserId,
      input.actor.actorEmail,
      input.action,
      input.targetType,
      input.targetId ?? null,
      JSON.stringify(input.requestPayload ?? {}),
      JSON.stringify(input.resultSummary ?? {})
    ]
  );
}

function maybeIso(value: Date | null): string | null {
  return value ? value.toISOString() : null;
}

function numberValue(value: string | number | null): number | null {
  return value == null ? null : Number(value);
}

async function fetchConversionJourney(shopifyOrderId: string) {
  const orderResult = await query<OrderJourneyRow>(
    `
      SELECT
        orders.shopify_order_id,
        orders.shopify_order_number,
        orders.shopify_customer_id,
        orders.currency_code,
        orders.subtotal_price,
        orders.total_price,
        orders.processed_at,
        orders.created_at_shopify,
        orders.landing_session_id::text AS landing_session_id,
        orders.checkout_token,
        orders.cart_token,
        orders.identity_journey_id::text AS identity_journey_id,
        attribution.attribution_model,
        attribution.attributed_source,
        attribution.attributed_medium,
        attribution.attributed_campaign,
        attribution.attributed_content,
        attribution.attributed_term,
        attribution.confidence_score,
        attribution.attribution_reason,
        attribution.attributed_at
      FROM shopify_orders orders
      LEFT JOIN attribution_results attribution
        ON attribution.shopify_order_id = orders.shopify_order_id
      WHERE orders.shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );
  const order = orderResult.rows[0];

  if (!order) {
    throw new AdminDebugHttpError(404, 'order_not_found', 'Shopify order was not found');
  }

  const runResult = await query<LatestAttributionRunRow>(
    `
      SELECT
        runs.id::text AS run_id,
        runs.run_status,
        runs.trigger_source,
        runs.created_at_utc,
        runs.completed_at_utc,
        inputs.order_occurred_at_utc,
        inputs.identity_journey_id::text AS identity_journey_id
      FROM attribution_order_inputs inputs
      INNER JOIN attribution_runs runs
        ON runs.id = inputs.run_id
      WHERE inputs.order_id = $1
      ORDER BY runs.created_at_utc DESC
      LIMIT 1
    `,
    [shopifyOrderId]
  );
  const latestRun = runResult.rows[0] ?? null;
  const runId = latestRun?.run_id ?? null;
  const journeyId = latestRun?.identity_journey_id ?? order.identity_journey_id;

  const [
    touchpointResult,
    summaryResult,
    creditResult,
    explainResult,
    trackingEventResult,
    identityJourneyResult,
    identityEdgeResult,
    mergeAuditResult
  ] = await Promise.all([
    runId
      ? query<AttributionTouchpointRow>(
          `
            SELECT
              touchpoint_id,
              session_id::text AS session_id,
              identity_journey_id::text AS identity_journey_id,
              touchpoint_occurred_at_utc,
              touchpoint_captured_at_utc,
              touchpoint_source_kind,
              ingestion_source,
              source,
              medium,
              campaign,
              content,
              term,
              click_id_type,
              click_id_value,
              evidence_source,
              is_direct,
              engagement_type,
              is_synthetic,
              is_eligible,
              ineligibility_reason,
              attribution_reason,
              attribution_hint
            FROM attribution_touchpoint_inputs
            WHERE run_id = $1::uuid
              AND order_id = $2
            ORDER BY touchpoint_occurred_at_utc ASC, touchpoint_id ASC
          `,
          [runId, shopifyOrderId]
        )
      : Promise.resolve({ rows: [] }),
    runId
      ? query<AttributionSummaryRow>(
          `
            SELECT
              model_key,
              allocation_status,
              winner_touchpoint_id,
              winner_session_id::text AS winner_session_id,
              winner_evidence_source,
              winner_attribution_reason,
              total_credit_weight,
              total_revenue_credited,
              touchpoint_count_considered,
              eligible_click_count,
              eligible_view_count,
              lookback_rule_applied,
              winner_selection_rule,
              direct_suppression_applied,
              deterministic_block_applied,
              normalization_failures_count
            FROM attribution_model_summaries
            WHERE run_id = $1::uuid
              AND order_id = $2
            ORDER BY model_key ASC
          `,
          [runId, shopifyOrderId]
        )
      : Promise.resolve({ rows: [] }),
    runId
      ? query<AttributionCreditRow>(
          `
            SELECT
              model_key,
              touchpoint_id,
              session_id::text AS session_id,
              touchpoint_position,
              occurred_at_utc,
              source,
              medium,
              campaign,
              evidence_source,
              attribution_reason,
              credit_weight,
              revenue_credit,
              is_primary,
              confidence_label
            FROM attribution_model_credits
            WHERE run_id = $1::uuid
              AND order_id = $2
            ORDER BY model_key ASC, touchpoint_position ASC
          `,
          [runId, shopifyOrderId]
        )
      : Promise.resolve({ rows: [] }),
    runId
      ? query<AttributionExplainRow>(
          `
            SELECT
              touchpoint_id,
              model_key,
              explain_stage,
              decision,
              decision_reason,
              details_json,
              created_at_utc
            FROM attribution_explain_records
            WHERE run_id = $1::uuid
              AND order_id = $2
            ORDER BY created_at_utc ASC, id ASC
          `,
          [runId, shopifyOrderId]
        )
      : Promise.resolve({ rows: [] }),
    query<TrackingEventRow>(
      `
        SELECT
          'tracking_events' AS source_table,
          events.id::text AS id,
          events.session_id::text AS session_id,
          events.event_type,
          events.occurred_at,
          events.page_url,
          events.referrer_url,
          events.utm_source,
          events.utm_medium,
          events.utm_campaign,
          events.gclid,
          events.fbclid,
          events.shopify_cart_token,
          events.shopify_checkout_token,
          NULL::text AS ingestion_source
        FROM tracking_events events
        WHERE ($1::uuid IS NOT NULL AND events.session_id = $1::uuid)
           OR ($2::text IS NOT NULL AND events.shopify_checkout_token = $2)
           OR ($3::text IS NOT NULL AND events.shopify_cart_token = $3)
        UNION ALL
        SELECT
          'session_attribution_touch_events' AS source_table,
          touch.id::text AS id,
          touch.roas_radar_session_id::text AS session_id,
          touch.event_type,
          touch.occurred_at,
          touch.page_url,
          touch.referrer_url,
          touch.utm_source,
          touch.utm_medium,
          touch.utm_campaign,
          touch.gclid,
          touch.fbclid,
          touch.shopify_cart_token,
          touch.shopify_checkout_token,
          touch.ingestion_source
        FROM session_attribution_touch_events touch
        WHERE ($1::uuid IS NOT NULL AND touch.roas_radar_session_id = $1::uuid)
           OR ($2::text IS NOT NULL AND touch.shopify_checkout_token = $2)
           OR ($3::text IS NOT NULL AND touch.shopify_cart_token = $3)
        ORDER BY occurred_at ASC, id ASC
        LIMIT 200
      `,
      [order.landing_session_id, order.checkout_token, order.cart_token]
    ),
    journeyId
      ? query<IdentityJourneyRow>(
          `
            SELECT
              id::text AS id,
              authoritative_shopify_customer_id,
              status,
              merge_version,
              merged_into_journey_id::text AS merged_into_journey_id,
              primary_email_hash,
              primary_phone_hash,
              lookback_window_started_at,
              lookback_window_expires_at,
              last_touch_eligible_at,
              first_source_system,
              last_source_system,
              created_at,
              updated_at
            FROM identity_journeys
            WHERE id = $1::uuid
            LIMIT 1
          `,
          [journeyId]
        )
      : Promise.resolve({ rows: [] }),
    journeyId
      ? query<IdentityEdgeRow>(
          `
            SELECT
              edges.id::text AS edge_id,
              nodes.node_type,
              nodes.node_key,
              edges.edge_type,
              edges.precedence_rank,
              edges.evidence_source,
              edges.source_table,
              edges.source_record_id,
              edges.is_active,
              edges.conflict_code,
              edges.first_observed_at,
              edges.last_observed_at
            FROM identity_edges edges
            INNER JOIN identity_nodes nodes
              ON nodes.id = edges.node_id
            WHERE edges.journey_id = $1::uuid
            ORDER BY edges.is_active DESC, edges.precedence_rank DESC, edges.last_observed_at DESC
          `,
          [journeyId]
        )
      : Promise.resolve({ rows: [] }),
    journeyId
      ? query<MergeAuditRow>(
          `
            SELECT
              id::text AS id,
              winner_journey_id::text AS winner_journey_id,
              loser_journey_id::text AS loser_journey_id,
              merge_reason_code,
              evidence_source,
              source_table,
              source_record_id,
              source_timestamp,
              winner_score,
              loser_score,
              candidate_scores,
              rehomed_nodes,
              quarantined_nodes,
              created_at
            FROM identity_journey_merge_audits
            WHERE winner_journey_id = $1::uuid
               OR loser_journey_id = $1::uuid
            ORDER BY created_at DESC
            LIMIT 50
          `,
          [journeyId]
        )
      : Promise.resolve({ rows: [] })
  ]);

  return {
    order: {
      shopifyOrderId: order.shopify_order_id,
      shopifyOrderNumber: order.shopify_order_number,
      shopifyCustomerId: order.shopify_customer_id,
      currencyCode: order.currency_code,
      subtotalPrice: Number(order.subtotal_price),
      totalPrice: Number(order.total_price),
      processedAt: maybeIso(order.processed_at),
      createdAtShopify: maybeIso(order.created_at_shopify),
      landingSessionId: order.landing_session_id,
      checkoutToken: order.checkout_token,
      cartToken: order.cart_token,
      identityJourneyId: journeyId,
      currentAttribution: {
        attributionModel: order.attribution_model,
        source: order.attributed_source,
        medium: order.attributed_medium,
        campaign: order.attributed_campaign,
        content: order.attributed_content,
        term: order.attributed_term,
        confidenceScore: numberValue(order.confidence_score),
        attributionReason: order.attribution_reason,
        attributedAt: maybeIso(order.attributed_at)
      }
    },
    run: latestRun
      ? {
          runId: latestRun.run_id,
          status: latestRun.run_status,
          triggerSource: latestRun.trigger_source,
          createdAt: latestRun.created_at_utc.toISOString(),
          completedAt: maybeIso(latestRun.completed_at_utc),
          orderOccurredAt: latestRun.order_occurred_at_utc.toISOString()
        }
      : null,
    events: trackingEventResult.rows.map((row) => ({
      sourceTable: row.source_table,
      id: row.id,
      sessionId: row.session_id,
      eventType: row.event_type,
      occurredAt: row.occurred_at.toISOString(),
      pageUrl: row.page_url,
      referrerUrl: row.referrer_url,
      utmSource: row.utm_source,
      utmMedium: row.utm_medium,
      utmCampaign: row.utm_campaign,
      gclid: row.gclid,
      fbclid: row.fbclid,
      shopifyCartToken: row.shopify_cart_token,
      shopifyCheckoutToken: row.shopify_checkout_token,
      ingestionSource: row.ingestion_source
    })),
    identity: {
      journey: identityJourneyResult.rows[0]
        ? {
            id: identityJourneyResult.rows[0].id,
            authoritativeShopifyCustomerId: identityJourneyResult.rows[0].authoritative_shopify_customer_id,
            status: identityJourneyResult.rows[0].status,
            mergeVersion: identityJourneyResult.rows[0].merge_version,
            mergedIntoJourneyId: identityJourneyResult.rows[0].merged_into_journey_id,
            primaryEmailHash: identityJourneyResult.rows[0].primary_email_hash,
            primaryPhoneHash: identityJourneyResult.rows[0].primary_phone_hash,
            lookbackWindowStartedAt: identityJourneyResult.rows[0].lookback_window_started_at.toISOString(),
            lookbackWindowExpiresAt: identityJourneyResult.rows[0].lookback_window_expires_at.toISOString(),
            lastTouchEligibleAt: identityJourneyResult.rows[0].last_touch_eligible_at.toISOString(),
            firstSourceSystem: identityJourneyResult.rows[0].first_source_system,
            lastSourceSystem: identityJourneyResult.rows[0].last_source_system,
            createdAt: identityJourneyResult.rows[0].created_at.toISOString(),
            updatedAt: identityJourneyResult.rows[0].updated_at.toISOString()
          }
        : null,
      edges: identityEdgeResult.rows.map((row) => ({
        edgeId: row.edge_id,
        nodeType: row.node_type,
        nodeKey: row.node_key,
        edgeType: row.edge_type,
        precedenceRank: row.precedence_rank,
        evidenceSource: row.evidence_source,
        sourceTable: row.source_table,
        sourceRecordId: row.source_record_id,
        isActive: row.is_active,
        conflictCode: row.conflict_code,
        firstObservedAt: row.first_observed_at.toISOString(),
        lastObservedAt: row.last_observed_at.toISOString()
      })),
      mergeAudits: mergeAuditResult.rows.map((row) => ({
        id: row.id,
        winnerJourneyId: row.winner_journey_id,
        loserJourneyId: row.loser_journey_id,
        mergeReasonCode: row.merge_reason_code,
        evidenceSource: row.evidence_source,
        sourceTable: row.source_table,
        sourceRecordId: row.source_record_id,
        sourceTimestamp: row.source_timestamp.toISOString(),
        winnerScore: row.winner_score,
        loserScore: row.loser_score,
        candidateScores: row.candidate_scores,
        rehomedNodes: row.rehomed_nodes,
        quarantinedNodes: row.quarantined_nodes,
        createdAt: row.created_at.toISOString()
      }))
    },
    attribution: {
      touchpoints: touchpointResult.rows.map((row) => ({
        touchpointId: row.touchpoint_id,
        sessionId: row.session_id,
        identityJourneyId: row.identity_journey_id,
        occurredAt: row.touchpoint_occurred_at_utc.toISOString(),
        capturedAt: row.touchpoint_captured_at_utc.toISOString(),
        sourceKind: row.touchpoint_source_kind,
        ingestionSource: row.ingestion_source,
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        content: row.content,
        term: row.term,
        clickIdType: row.click_id_type,
        clickIdValue: row.click_id_value,
        evidenceSource: row.evidence_source,
        isDirect: row.is_direct,
        engagementType: row.engagement_type,
        isSynthetic: row.is_synthetic,
        isEligible: row.is_eligible,
        ineligibilityReason: row.ineligibility_reason,
        attributionReason: row.attribution_reason,
        attributionHint: row.attribution_hint
      })),
      modelSummaries: summaryResult.rows.map((row) => ({
        modelKey: row.model_key,
        allocationStatus: row.allocation_status,
        winnerTouchpointId: row.winner_touchpoint_id,
        winnerSessionId: row.winner_session_id,
        winnerEvidenceSource: row.winner_evidence_source,
        winnerAttributionReason: row.winner_attribution_reason,
        totalCreditWeight: Number(row.total_credit_weight),
        totalRevenueCredited: Number(row.total_revenue_credited),
        touchpointCountConsidered: row.touchpoint_count_considered,
        eligibleClickCount: row.eligible_click_count,
        eligibleViewCount: row.eligible_view_count,
        lookbackRuleApplied: row.lookback_rule_applied,
        winnerSelectionRule: row.winner_selection_rule,
        directSuppressionApplied: row.direct_suppression_applied,
        deterministicBlockApplied: row.deterministic_block_applied,
        normalizationFailuresCount: row.normalization_failures_count
      })),
      credits: creditResult.rows.map((row) => ({
        modelKey: row.model_key,
        touchpointId: row.touchpoint_id,
        sessionId: row.session_id,
        touchpointPosition: row.touchpoint_position,
        occurredAt: row.occurred_at_utc.toISOString(),
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        evidenceSource: row.evidence_source,
        attributionReason: row.attribution_reason,
        creditWeight: Number(row.credit_weight),
        revenueCredit: Number(row.revenue_credit),
        isPrimary: row.is_primary,
        confidenceLabel: row.confidence_label
      })),
      explainRecords: explainResult.rows.map((row) => ({
        touchpointId: row.touchpoint_id,
        modelKey: row.model_key,
        explainStage: row.explain_stage,
        decision: row.decision,
        decisionReason: row.decision_reason,
        details: row.details_json,
        createdAt: row.created_at_utc.toISOString()
      }))
    }
  };
}

async function fetchAuditLog(input: z.infer<typeof auditQuerySchema>) {
  const result = await query<AuditLogRow>(
    `
      SELECT
        id::text AS id,
        actor_kind,
        actor_user_id,
        actor_email,
        action,
        target_type,
        target_id,
        request_payload,
        result_summary,
        created_at
      FROM admin_debug_audit_log
      WHERE ($1::text IS NULL OR action = $1)
        AND ($2::text IS NULL OR target_type = $2)
        AND ($3::text IS NULL OR target_id = $3)
      ORDER BY created_at DESC, id DESC
      LIMIT $4
    `,
    [input.action ?? null, input.targetType ?? null, input.targetId ?? null, input.limit]
  );

  return {
    rows: result.rows.map((row) => ({
      id: row.id,
      actorKind: row.actor_kind,
      actorUserId: row.actor_user_id == null ? null : Number(row.actor_user_id),
      actorEmail: row.actor_email,
      action: row.action,
      targetType: row.target_type,
      targetId: row.target_id,
      requestPayload: row.request_payload,
      resultSummary: row.result_summary,
      createdAt: row.created_at.toISOString()
    }))
  };
}

export function createAttributionAdminDebugRouter(): Router {
  const router = Router();

  router.get('/journeys/:shopifyOrderId', async (req, res, next) => {
    try {
      const actor = getActor(res.locals.auth as AuthContext | null | undefined);
      const response = await fetchConversionJourney(req.params.shopifyOrderId);
      await writeAdminDebugAudit({
        actor,
        action: 'conversion_journey_inspect',
        targetType: 'shopify_order',
        targetId: req.params.shopifyOrderId,
        resultSummary: {
          runId: response.run?.runId ?? null,
          touchpointCount: response.attribution.touchpoints.length,
          identityEdgeCount: response.identity.edges.length,
          mergeAuditCount: response.identity.mergeAudits.length
        }
      });

      res.status(200).json(response);
    } catch (error) {
      next(error);
    }
  });

  router.post('/campaign-resolver', async (req, res, next) => {
    try {
      const actor = getActor(res.locals.auth as AuthContext | null | undefined);
      const payload = parseInput(
        campaignResolverRequestSchema.extend({
          enqueueUnmapped: z.boolean().optional().default(false)
        }),
        req.body ?? {},
        'Invalid campaign resolver debug request'
      );
      const resolution = await resolveCampaignMetadata({
        ...payload,
        enqueueUnmapped: payload.enqueueUnmapped ?? false
      });

      await writeAdminDebugAudit({
        actor,
        action: 'campaign_resolver_debug',
        targetType: 'campaign_metadata',
        targetId: payload.campaignId ?? payload.campaign ?? null,
        requestPayload: payload,
        resultSummary: {
          status: resolution.status,
          source: resolution.source,
          ruleId: resolution.ruleId,
          qaQueueId: resolution.qaQueueId
        }
      });

      res.status(200).json({ resolution });
    } catch (error) {
      next(error);
    }
  });

  router.post('/replay', async (req, res, next) => {
    try {
      const actor = getActor(res.locals.auth as AuthContext | null | undefined);
      const payload = parseInput(replayTriggerSchema, req.body ?? {}, 'Invalid replay request');
      const replay = await replayDeadLetters({
        ...payload,
        fromTime: payload.fromTime ? new Date(payload.fromTime) : undefined,
        toTime: payload.toTime ? new Date(payload.toTime) : undefined,
        requestedBy: actor.actorEmail
      });

      await writeAdminDebugAudit({
        actor,
        action: payload.dryRun ? 'dead_letter_replay_dry_run' : 'dead_letter_replay',
        targetType: 'event_dead_letters',
        targetId: replay.replayRunId == null ? null : String(replay.replayRunId),
        requestPayload: payload,
        resultSummary: replay
      });

      res.status(202).json({ replay });
    } catch (error) {
      next(error);
    }
  });

  router.post('/recompute', async (req, res, next) => {
    try {
      const actor = getActor(res.locals.auth as AuthContext | null | undefined);
      const payload = parseBackfillRequest(req.body ?? {});
      const response = await enqueueOrderAttributionBackfillRun(payload, actor.actorEmail);

      await writeAdminDebugAudit({
        actor,
        action: 'order_attribution_recompute',
        targetType: 'order_attribution_backfill_run',
        targetId: response.jobId,
        requestPayload: payload,
        resultSummary: {
          jobId: response.jobId,
          status: response.status,
          submittedAt: response.submittedAt
        }
      });

      res.status(202).json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/audit', async (req, res, next) => {
    try {
      const input = parseInput(auditQuerySchema, req.query, 'Invalid audit query');
      const response = await fetchAuditLog(input);
      res.status(200).json(response);
    } catch (error) {
      next(error);
    }
  });

  return router;
}
