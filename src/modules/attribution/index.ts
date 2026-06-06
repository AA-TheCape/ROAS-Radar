import type { PoolClient } from 'pg';

import { withTransaction } from '../../db/pool.js';
import {
  emitGoogleCpcMissingCampaignNameAttributionWriteLog,
  emitAttributionQaSnapshotWriteLog,
  emitAttributionResolverOutcomeLog,
  logError
} from '../../observability/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { formatDateInTimezone, getReportingTimezone } from '../settings/index.js';
import {
  collectDeterministicFirstPartyCandidates,
  extractAttributionCandidatesForOrder,
  type AttributionCandidateExtractionResult
} from './candidate-extraction.js';
import {
  ATTRIBUTION_MODELS,
  executeAttributionModels,
  type AttributionCredit
} from './engine.js';
import { resolveActiveAttributionLookupPair } from './attribution-lookups.js';
import {
  attributionConfidenceFingerprintChanged,
  boundConfidenceScore,
  buildAttributionConfidenceMetadata,
  type AttributionConfidenceFingerprint,
  type PersistedAttributionConfidenceState
} from './confidence-scoring.js';
import {
  buildAttributionConfidenceLabel,
  buildAttributionMatchSource,
  buildOrderAttributionAuditRecord
} from './order-attribution-audit.js';
import { buildAttributionQaSnapshot } from './qa-snapshot.js';
import {
  confidenceScoreForWinner,
  dedupeDeterministicCandidates,
  isDirectTouchpoint,
  resolveAttributionTier,
  selectLastNonDirectWinner,
  type ResolvedJourney,
  type ResolvedAttributionTouchpoint
} from './resolver.js';
import {
  loadAttributionPreprocessingSnapshot,
  preprocessAttributionOrders,
  preprocessAttributionSnapshot
} from './preprocessing.js';
import {
  type AttributionComparableFields,
  classifyAttributionOrigin,
  shouldApplyAttributionUpdate
} from './precedence.js';

const ATTRIBUTION_MODEL_VERSION = 1;
const JOB_STALE_AFTER_MINUTES = 15;
const MAX_RETRY_DELAY_SECONDS = 1_800;

type OrderRow = {
  shopify_order_id: string;
  name: string | null;
  currency_code: string | null;
  subtotal_price: string | null;
  total_price: string;
  processed_at: Date | null;
  created_at_shopify: Date | null;
  ingested_at: Date;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  shopify_customer_id: string | null;
  email_hash: string | null;
  customer_identity_id: string | null;
  identity_journey_id: string | null;
  source_name: string | null;
  raw_payload: unknown;
};

type CurrentAttributionRow = {
  session_id: string | null;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attributed_campaign_id: string | null;
  attributed_content: string | null;
  attributed_term: string | null;
  attributed_click_id_type: string | null;
  attributed_click_id_value: string | null;
  attributed_account_id: string | null;
  attributed_account_name: string | null;
  attributed_channel_type: string | null;
  attributed_channel_subtype: string | null;
  attributed_campaign_metadata_source: string | null;
  attributed_account_metadata_source: string | null;
  attributed_channel_metadata_source: string | null;
  attribution_reason: string | null;
  match_source: string | null;
  order_attribution_tier: string | null;
  order_attribution_source: string | null;
  order_attribution_reason: string | null;
};

type ResolvedAttributionJourney = {
  journey: ResolvedJourney;
  candidateEvaluation: AttributionCandidateExtractionResult;
};

type ClaimedAttributionJob = {
  id: number;
  shopify_order_id: string;
  attempts: number;
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
  matchSource?: string | null;
  attributionReason: string;
  confidenceScore?: number | null;
};

type PersistedAttributionTouchpoint = Pick<
  AttributionCredit,
  | 'sessionId'
  | 'source'
  | 'medium'
  | 'campaign'
  | 'campaignId'
  | 'content'
  | 'term'
  | 'clickIdType'
  | 'clickIdValue'
  | 'accountId'
  | 'accountName'
  | 'channelType'
  | 'channelSubtype'
  | 'campaignMetadataSource'
  | 'accountMetadataSource'
  | 'channelMetadataSource'
  | 'attributionReason'
>;

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
      RETURNING j.id, j.shopify_order_id, j.attempts
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
        shopify_order_number AS name,
        currency_code,
        subtotal_price::text AS subtotal_price,
        total_price::text AS total_price,
        processed_at,
        created_at_shopify,
        ingested_at,
        landing_session_id::text AS landing_session_id,
        checkout_token,
        cart_token,
        shopify_customer_id,
        email_hash,
        customer_identity_id::text AS customer_identity_id,
        identity_journey_id::text AS identity_journey_id,
        source_name,
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
    campaignId: 'campaignId' in touchpoint ? normalizeNullableString(touchpoint.campaignId) : null,
    content: touchpoint.content,
    term: touchpoint.term,
    clickIdType: touchpoint.clickIdType,
    clickIdValue: touchpoint.clickIdValue,
    accountId: 'accountId' in touchpoint ? normalizeNullableString(touchpoint.accountId) : null,
    accountName: 'accountName' in touchpoint ? normalizeNullableString(touchpoint.accountName) : null,
    channelType: 'channelType' in touchpoint ? normalizeNullableString(touchpoint.channelType) : null,
    channelSubtype: 'channelSubtype' in touchpoint ? normalizeNullableString(touchpoint.channelSubtype) : null,
    campaignMetadataSource:
      'campaignMetadataSource' in touchpoint ? normalizeNullableString(touchpoint.campaignMetadataSource) : null,
    accountMetadataSource:
      'accountMetadataSource' in touchpoint ? normalizeNullableString(touchpoint.accountMetadataSource) : null,
    channelMetadataSource:
      'channelMetadataSource' in touchpoint ? normalizeNullableString(touchpoint.channelMetadataSource) : null,
    attributionReason: touchpoint.attributionReason,
    ingestionSource: touchpoint.ingestionSource,
    isDirect: touchpoint.isDirect
  };
}

async function fetchCurrentAttribution(
  client: PoolClient,
  shopifyOrderId: string
): Promise<CurrentAttributionRow | null> {
  const result = await client.query<CurrentAttributionRow>(
    `
      SELECT
        results.session_id::text,
        results.attributed_source,
        results.attributed_medium,
        results.attributed_campaign,
        results.attributed_campaign_id,
        results.attributed_content,
        results.attributed_term,
        results.attributed_click_id_type,
        results.attributed_click_id_value,
        results.attributed_account_id,
        results.attributed_account_name,
        results.attributed_channel_type,
        results.attributed_channel_subtype,
        results.attributed_campaign_metadata_source,
        results.attributed_account_metadata_source,
        results.attributed_channel_metadata_source,
        results.attribution_reason,
        results.match_source,
        orders.attribution_tier AS order_attribution_tier,
        orders.attribution_source AS order_attribution_source,
        orders.attribution_reason AS order_attribution_reason
      FROM shopify_orders orders
      LEFT JOIN attribution_results results
        ON results.shopify_order_id = orders.shopify_order_id
      WHERE orders.shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  return result.rows[0] ?? null;
}

function buildCurrentAttributionComparable(row: CurrentAttributionRow): AttributionComparableFields {
  return {
    sessionId: row.session_id,
    source: row.attributed_source,
    medium: row.attributed_medium,
    campaign: row.attributed_campaign,
    campaignId: row.attributed_campaign_id,
    content: row.attributed_content,
    term: row.attributed_term,
    clickIdType: row.attributed_click_id_type,
    clickIdValue: row.attributed_click_id_value,
    accountId: row.attributed_account_id,
    accountName: row.attributed_account_name,
    channelType: row.attributed_channel_type,
    channelSubtype: row.attributed_channel_subtype,
    campaignMetadataSource: row.attributed_campaign_metadata_source,
    accountMetadataSource: row.attributed_account_metadata_source,
    channelMetadataSource: row.attributed_channel_metadata_source,
    attributionReason: row.attribution_reason
  };
}

function buildProposedAttributionComparable(
  primaryCredit: Pick<
    AttributionCredit,
    | 'sessionId'
    | 'source'
    | 'medium'
    | 'campaign'
    | 'campaignId'
    | 'content'
    | 'term'
    | 'clickIdType'
    | 'clickIdValue'
    | 'accountId'
    | 'accountName'
    | 'channelType'
    | 'channelSubtype'
    | 'campaignMetadataSource'
    | 'accountMetadataSource'
    | 'channelMetadataSource'
    | 'attributionReason'
  > | null,
  journey: ResolvedJourney
): AttributionComparableFields {
  return {
    sessionId: primaryCredit?.sessionId ?? null,
    source: normalizeNullableString(primaryCredit?.source),
    medium: normalizeNullableString(primaryCredit?.medium),
    campaign: normalizeNullableString(primaryCredit?.campaign),
    campaignId: normalizeNullableString(primaryCredit?.campaignId),
    content: normalizeNullableString(primaryCredit?.content),
    term: normalizeNullableString(primaryCredit?.term),
    clickIdType: normalizeNullableString(primaryCredit?.clickIdType),
    clickIdValue: normalizeNullableString(primaryCredit?.clickIdValue),
    accountId: normalizeNullableString(primaryCredit?.accountId),
    accountName: normalizeNullableString(primaryCredit?.accountName),
    channelType: normalizeNullableString(primaryCredit?.channelType),
    channelSubtype: normalizeNullableString(primaryCredit?.channelSubtype),
    campaignMetadataSource: normalizeNullableString(primaryCredit?.campaignMetadataSource),
    accountMetadataSource: normalizeNullableString(primaryCredit?.accountMetadataSource),
    channelMetadataSource: normalizeNullableString(primaryCredit?.channelMetadataSource),
    attributionReason: primaryCredit?.attributionReason ?? journey.attributionReason
  };
}

async function resolveAttributionJourney(client: PoolClient, order: OrderRow): Promise<ResolvedAttributionJourney> {
  const candidateEvaluation = await extractAttributionCandidatesForOrder(client, {
    shopifyOrderId: order.shopify_order_id,
    processedAt: order.processed_at,
    createdAtShopify: order.created_at_shopify,
    ingestedAt: order.ingested_at,
    landingSessionId: order.landing_session_id,
    checkoutToken: order.checkout_token,
    cartToken: order.cart_token,
    emailHash: order.email_hash,
    customerIdentityId: order.customer_identity_id,
    identityJourneyId: order.identity_journey_id,
    sourceName: order.source_name,
    rawPayload: order.raw_payload
  });

  return {
    journey: resolveAttributionTier(candidateEvaluation),
    candidateEvaluation
  };
}

function selectPrimaryCredit(credits: AttributionCredit[]): AttributionCredit | undefined {
  return credits.find((credit) => credit.isPrimary) ?? credits[credits.length - 1];
}

function selectPersistedPrimaryTouchpoint(
  outputs: Record<(typeof ATTRIBUTION_MODELS)[number], AttributionCredit[]>,
  journey: ResolvedJourney
): PersistedAttributionTouchpoint | null {
  const persistedCredit =
    selectPrimaryCredit(outputs.last_non_direct) ??
    selectPrimaryCredit(outputs.hinted_fallback_only);

  if (persistedCredit) {
    return persistedCredit;
  }

  if (!journey.winner) {
    return null;
  }

  return {
    sessionId: journey.winner.sessionId,
    source: journey.winner.source,
    medium: journey.winner.medium,
    campaign: journey.winner.campaign,
    campaignId: 'campaignId' in journey.winner ? normalizeNullableString(journey.winner.campaignId) : null,
    content: journey.winner.content,
    term: journey.winner.term,
    clickIdType: journey.winner.clickIdType,
    clickIdValue: journey.winner.clickIdValue,
    accountId: 'accountId' in journey.winner ? normalizeNullableString(journey.winner.accountId) : null,
    accountName: 'accountName' in journey.winner ? normalizeNullableString(journey.winner.accountName) : null,
    channelType: 'channelType' in journey.winner ? normalizeNullableString(journey.winner.channelType) : null,
    channelSubtype: 'channelSubtype' in journey.winner ? normalizeNullableString(journey.winner.channelSubtype) : null,
    campaignMetadataSource:
      'campaignMetadataSource' in journey.winner ? normalizeNullableString(journey.winner.campaignMetadataSource) : null,
    accountMetadataSource:
      'accountMetadataSource' in journey.winner ? normalizeNullableString(journey.winner.accountMetadataSource) : null,
    channelMetadataSource:
      'channelMetadataSource' in journey.winner ? normalizeNullableString(journey.winner.channelMetadataSource) : null,
    attributionReason: journey.winner.attributionReason
  };
}

async function fetchPersistedAttributionConfidenceState(
  client: PoolClient,
  shopifyOrderId: string
): Promise<PersistedAttributionConfidenceState | null> {
  const result = await client.query<{
    session_id: string | null;
    attributed_source: string | null;
    attributed_medium: string | null;
    attributed_campaign: string | null;
    attributed_content: string | null;
    attributed_term: string | null;
    attributed_click_id_type: string | null;
    attributed_click_id_value: string | null;
    confidence_score: string;
    attribution_reason: string;
    model_version: number;
    match_source: string;
    attribution_source_code: string | null;
    matching_method_code: string | null;
    confidence_contract_version: string | null;
    last_attribution_run_at: Date | null;
  }>(
    `
      SELECT
        results.session_id::text AS session_id,
        results.attributed_source,
        results.attributed_medium,
        results.attributed_campaign,
        results.attributed_content,
        results.attributed_term,
        results.attributed_click_id_type,
        results.attributed_click_id_value,
        results.confidence_score::text,
        results.attribution_reason,
        results.model_version,
        results.match_source,
        sources.code AS attribution_source_code,
        methods.code AS matching_method_code,
        results.confidence_contract_version,
        results.last_attribution_run_at
      FROM attribution_results results
      LEFT JOIN attribution_sources sources
        ON sources.id = results.attribution_source_id
      LEFT JOIN matching_methods methods
        ON methods.id = results.matching_method_id
      WHERE results.shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  const row = result.rows[0];

  if (!row) {
    return null;
  }

  return {
    sessionId: row.session_id,
    attributedSource: row.attributed_source,
    attributedMedium: row.attributed_medium,
    attributedCampaign: row.attributed_campaign,
    attributedContent: row.attributed_content,
    attributedTerm: row.attributed_term,
    attributedClickIdType: row.attributed_click_id_type,
    attributedClickIdValue: row.attributed_click_id_value,
    confidenceScore: boundConfidenceScore(Number(row.confidence_score)),
    attributionReason: row.attribution_reason,
    modelVersion: row.model_version,
    matchSource: row.match_source,
    attributionSourceCode: row.attribution_source_code ?? 'unattributed',
    matchingMethodCode: row.matching_method_code ?? 'unknown',
    confidenceContractVersion: row.confidence_contract_version ?? 'v1',
    lastAttributionRunAt: row.last_attribution_run_at
  };
}

function buildAttributionConfidenceFingerprint(input: {
  primaryCredit: PersistedAttributionTouchpoint | null;
  journey: ResolvedJourney;
  confidenceScore: number;
  matchSource: string;
  attributionSourceCode: string;
}): AttributionConfidenceFingerprint {
  const attributionReason = input.primaryCredit?.attributionReason ?? input.journey.attributionReason;

  return {
    sessionId: input.primaryCredit?.sessionId ?? null,
    attributedSource: normalizeNullableString(input.primaryCredit?.source),
    attributedMedium: normalizeNullableString(input.primaryCredit?.medium),
    attributedCampaign: normalizeNullableString(input.primaryCredit?.campaign),
    attributedContent: normalizeNullableString(input.primaryCredit?.content),
    attributedTerm: normalizeNullableString(input.primaryCredit?.term),
    attributedClickIdType: normalizeNullableString(input.primaryCredit?.clickIdType),
    attributedClickIdValue: normalizeNullableString(input.primaryCredit?.clickIdValue),
    confidenceScore: input.confidenceScore,
    attributionReason,
    modelVersion: ATTRIBUTION_MODEL_VERSION,
    matchSource: input.matchSource,
    attributionSourceCode: input.attributionSourceCode,
    matchingMethodCode: attributionReason,
    confidenceContractVersion: 'v1'
  };
}

async function persistAttribution(
  client: PoolClient,
  order: OrderRow,
  resolved: ResolvedAttributionJourney
): Promise<boolean> {
  const { journey, candidateEvaluation } = resolved;
  const orderOccurredAt = journey.orderOccurredAtUtc ?? resolveOrderOccurredAt(order);
  const execution = executeAttributionModels(journey.touchpoints, {
    orderOccurredAt,
    orderRevenue: order.total_price,
    attributionModels: ATTRIBUTION_MODELS,
    normalizationFailuresCount: journey.normalizationFailures.length
  });
  const outputs = execution.creditsByModel;

  const primaryCredit = selectPersistedPrimaryTouchpoint(outputs, journey);

  const persistedConfidenceState = await fetchPersistedAttributionConfidenceState(client, order.shopify_order_id);
  const matchedAt = new Date();
  const orderAttributionAudit = buildOrderAttributionAuditRecord(journey, matchedAt);
  const proposedConfidenceMetadata = buildAttributionConfidenceMetadata({
    journey,
    attributionSourceCode: orderAttributionAudit.source,
    lastAttributionRunAt: matchedAt
  });
  const matchSource = buildAttributionMatchSource(journey);
  const confidenceFingerprint = buildAttributionConfidenceFingerprint({
    primaryCredit,
    journey,
    confidenceScore: proposedConfidenceMetadata.confidenceScore,
    matchSource,
    attributionSourceCode: proposedConfidenceMetadata.attributionSourceCode
  });
  const attributionChanged = attributionConfidenceFingerprintChanged(persistedConfidenceState, confidenceFingerprint);
  const confidenceMetadata =
    attributionChanged || !persistedConfidenceState?.lastAttributionRunAt
      ? proposedConfidenceMetadata
      : {
          confidenceScore: persistedConfidenceState.confidenceScore,
          attributionSourceCode: persistedConfidenceState.attributionSourceCode,
          matchingMethodCode: persistedConfidenceState.matchingMethodCode,
          confidenceContractVersion: persistedConfidenceState.confidenceContractVersion,
          lastAttributionRunAt: persistedConfidenceState.lastAttributionRunAt
        };
  const confidenceLabel = buildAttributionConfidenceLabel(confidenceMetadata.confidenceScore);
  const current = await fetchCurrentAttribution(client, order.shopify_order_id);
  const shouldApply = shouldApplyAttributionUpdate({
    current: current
      ? {
          origin: classifyAttributionOrigin({
            attributionTier: current.order_attribution_tier,
            attributionSource: current.order_attribution_source,
            matchSource: current.match_source,
            attributionReason: current.attribution_reason ?? current.order_attribution_reason
          }),
          attribution: buildCurrentAttributionComparable(current)
        }
      : null,
    proposed: {
      origin: classifyAttributionOrigin({
        attributionTier: orderAttributionAudit.tier,
        attributionSource: orderAttributionAudit.source,
        matchSource,
        attributionReason: primaryCredit?.attributionReason ?? journey.attributionReason
      }),
      attribution: buildProposedAttributionComparable(primaryCredit, journey)
    }
  });

  if (!shouldApply) {
    return false;
  }

  const qaSnapshot = buildAttributionQaSnapshot({
    order,
    candidates: candidateEvaluation,
    journey,
    execution,
    generatedAt: matchedAt
  });
  const attributionSnapshot = {
    tier: journey.tier,
    attributionReason: journey.attributionReason,
    orderOccurredAtUtc: journey.orderOccurredAtUtc?.toISOString() ?? null,
    normalizationFailures: journey.normalizationFailures,
    confidenceScore: confidenceMetadata.confidenceScore,
    winner: journey.winner ? serializeResolvedTouchpoint(journey.winner) : null,
    timeline: journey.touchpoints.map(serializeResolvedTouchpoint),
    qaSnapshot
  };
  const lookupPair = await resolveActiveAttributionLookupPair(client, {
    attributionSourceCode: confidenceMetadata.attributionSourceCode,
    matchingMethodCode: confidenceMetadata.matchingMethodCode
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
            attributed_campaign_id,
            attributed_content,
            attributed_term,
            attributed_click_id_type,
            attributed_click_id_value,
            attributed_account_id,
            attributed_account_name,
            attributed_channel_type,
            attributed_channel_subtype,
            attributed_campaign_metadata_source,
            attributed_account_metadata_source,
            attributed_channel_metadata_source,
            credit_weight,
            revenue_credit,
            is_primary,
            attribution_reason,
            model_version,
            match_source,
            confidence_label,
            confidence_contract_version
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
            $19,
            $20,
            $21,
            $22,
            $23,
	            $24,
	            $25,
	            $26,
	            $27,
	            $28
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
          normalizeNullableString(credit.campaignId),
          normalizeNullableString(credit.content),
          normalizeNullableString(credit.term),
          normalizeNullableString(credit.clickIdType),
          normalizeNullableString(credit.clickIdValue),
          normalizeNullableString(credit.accountId),
          normalizeNullableString(credit.accountName),
          normalizeNullableString(credit.channelType),
          normalizeNullableString(credit.channelSubtype),
          normalizeNullableString(credit.campaignMetadataSource),
          normalizeNullableString(credit.accountMetadataSource),
          normalizeNullableString(credit.channelMetadataSource),
          credit.creditWeight,
          credit.revenueCredit,
          credit.isPrimary,
          credit.attributionReason,
          ATTRIBUTION_MODEL_VERSION,
          matchSource,
          confidenceLabel,
          confidenceMetadata.confidenceContractVersion
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
        attributed_campaign_id,
        attributed_content,
        attributed_term,
        attributed_click_id_type,
        attributed_click_id_value,
        attributed_account_id,
        attributed_account_name,
        attributed_channel_type,
        attributed_channel_subtype,
        attributed_campaign_metadata_source,
        attributed_account_metadata_source,
        attributed_channel_metadata_source,
        confidence_score,
        attribution_reason,
        attributed_at,
        reprocess_version,
        model_version,
        match_source,
        confidence_label,
        attribution_source_id,
        matching_method_id,
        confidence_contract_version,
        last_attribution_run_at
      )
      VALUES (
        $1,
        $2::uuid,
        'last_non_direct',
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
	        $13,
	        $14,
	        $15,
	        $16,
	        $17,
	        $18,
	        $19,
	        $20,
	        1,
	        $21,
	        $22,
	        $23,
	        $24,
	        $25,
	        $26,
	        $20
      )
      ON CONFLICT (shopify_order_id)
      DO UPDATE SET
        session_id = EXCLUDED.session_id,
        attribution_model = EXCLUDED.attribution_model,
        attributed_source = EXCLUDED.attributed_source,
        attributed_medium = EXCLUDED.attributed_medium,
        attributed_campaign = EXCLUDED.attributed_campaign,
        attributed_campaign_id = EXCLUDED.attributed_campaign_id,
        attributed_content = EXCLUDED.attributed_content,
        attributed_term = EXCLUDED.attributed_term,
        attributed_click_id_type = EXCLUDED.attributed_click_id_type,
        attributed_click_id_value = EXCLUDED.attributed_click_id_value,
        attributed_account_id = EXCLUDED.attributed_account_id,
        attributed_account_name = EXCLUDED.attributed_account_name,
        attributed_channel_type = EXCLUDED.attributed_channel_type,
        attributed_channel_subtype = EXCLUDED.attributed_channel_subtype,
        attributed_campaign_metadata_source = EXCLUDED.attributed_campaign_metadata_source,
        attributed_account_metadata_source = EXCLUDED.attributed_account_metadata_source,
        attributed_channel_metadata_source = EXCLUDED.attributed_channel_metadata_source,
        confidence_score = EXCLUDED.confidence_score,
        attribution_reason = EXCLUDED.attribution_reason,
        attributed_at = EXCLUDED.attributed_at,
        model_version = EXCLUDED.model_version,
        match_source = EXCLUDED.match_source,
        confidence_label = EXCLUDED.confidence_label,
        attribution_source_id = EXCLUDED.attribution_source_id,
        matching_method_id = EXCLUDED.matching_method_id,
        confidence_contract_version = EXCLUDED.confidence_contract_version,
        last_attribution_run_at = EXCLUDED.last_attribution_run_at
    `,
    [
      order.shopify_order_id,
      primaryCredit?.sessionId ?? null,
      normalizeNullableString(primaryCredit?.source),
      normalizeNullableString(primaryCredit?.medium),
      normalizeNullableString(primaryCredit?.campaign),
      normalizeNullableString(primaryCredit?.campaignId),
      normalizeNullableString(primaryCredit?.content),
      normalizeNullableString(primaryCredit?.term),
      normalizeNullableString(primaryCredit?.clickIdType),
      normalizeNullableString(primaryCredit?.clickIdValue),
      normalizeNullableString(primaryCredit?.accountId),
      normalizeNullableString(primaryCredit?.accountName),
      normalizeNullableString(primaryCredit?.channelType),
      normalizeNullableString(primaryCredit?.channelSubtype),
      normalizeNullableString(primaryCredit?.campaignMetadataSource),
      normalizeNullableString(primaryCredit?.accountMetadataSource),
      normalizeNullableString(primaryCredit?.channelMetadataSource),
      confidenceMetadata.confidenceScore,
      primaryCredit?.attributionReason ?? journey.attributionReason,
      confidenceMetadata.lastAttributionRunAt,
      ATTRIBUTION_MODEL_VERSION,
      matchSource,
      confidenceLabel,
      lookupPair.attributionSourceId,
      lookupPair.matchingMethodId,
      confidenceMetadata.confidenceContractVersion
    ]
  );

  emitGoogleCpcMissingCampaignNameAttributionWriteLog({
    writePath: 'attribution_results_upsert',
    attributionModel: 'last_non_direct',
    source: primaryCredit?.source,
    medium: primaryCredit?.medium,
    effectiveCampaign: primaryCredit?.campaign,
    campaignId: primaryCredit?.campaignId,
    accountId: primaryCredit?.accountId,
    campaignMetadataSource: primaryCredit?.campaignMetadataSource,
    accountMetadataSource: primaryCredit?.accountMetadataSource
  });

  try {
    await client.query(
      `
        UPDATE shopify_orders
        SET
          attribution_tier = $2,
          attribution_source = $3,
          attribution_matched_at = $4,
          attribution_reason = $5,
          attribution_source_id = $9,
          matching_method_id = $10,
          attribution_confidence_score = $6,
          attribution_confidence_contract_version = $8,
          last_attribution_run_at = $4,
          attribution_snapshot = $7::jsonb,
          attribution_snapshot_updated_at = $4
        WHERE shopify_order_id = $1
      `,
      [
        order.shopify_order_id,
        orderAttributionAudit.tier,
        orderAttributionAudit.source,
        confidenceMetadata.lastAttributionRunAt,
        orderAttributionAudit.reason,
        confidenceMetadata.confidenceScore,
        JSON.stringify(attributionSnapshot),
        confidenceMetadata.confidenceContractVersion,
        lookupPair.attributionSourceId,
        lookupPair.matchingMethodId
      ]
    );
    emitAttributionQaSnapshotWriteLog({
      orderId: order.shopify_order_id,
      pipeline: 'realtime_queue',
      status: 'success',
      attributionTier: journey.tier,
      matchSource,
      payload: qaSnapshot
    });
  } catch (error) {
    emitAttributionQaSnapshotWriteLog({
      orderId: order.shopify_order_id,
      pipeline: 'realtime_queue',
      status: 'failure',
      attributionTier: journey.tier,
      matchSource,
      payload: qaSnapshot,
      error
    });
    throw error;
  }

  emitAttributionResolverOutcomeLog({
    shopifyOrderId: order.shopify_order_id,
    orderOccurredAtUtc: journey.orderOccurredAtUtc,
    tier: journey.tier,
    attributionReason: journey.attributionReason,
    confidenceScore: confidenceMetadata.confidenceScore,
    pipeline: 'realtime_queue',
    touchpoints: journey.touchpoints,
    winner: journey.winner,
    normalizationFailures: journey.normalizationFailures
  });

  return true;
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

  const resolved = await resolveAttributionJourney(client, order);
  const { journey } = resolved;

  await persistAttribution(client, order, resolved);

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
      confidenceScore: journey.confidenceScore,
      touchpointCount: journey.touchpoints.length,
      attributionReason: primaryCreditReason(journey)
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
      ingestionSource: input.matchSource === 'ga4_fallback' ? 'ga4_fallback' : 'shopify_marketing_hint',
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

    const syntheticTier = input.matchSource === 'ga4_fallback' ? 'ga4_fallback' : 'deterministic_shopify_hint';
    const confidenceScore =
      input.confidenceScore ??
      (input.matchSource === 'ga4_fallback' ? (normalizedClickIdValue ? 0.35 : 0.25) : normalizedClickIdValue ? 0.55 : 0.4);
    const syntheticCandidate = {
      sourceClass: syntheticTier,
      sourceKey: `synthetic:${shopifyOrderId}:${orderOccurredAt.toISOString()}`,
      sessionId: null,
      sourceTouchEventId: null,
      ingestionSource: syntheticTier === 'ga4_fallback' ? 'ga4_fallback' : 'shopify_marketing_hint',
      occurredAtUtc: touchpoint.occurredAt,
      source: touchpoint.source,
      medium: touchpoint.medium,
      campaign: touchpoint.campaign,
      content: touchpoint.content,
      term: touchpoint.term,
      clickIdType: touchpoint.clickIdType,
      clickIdValue: touchpoint.clickIdValue,
      attributionReason: touchpoint.attributionReason,
      confidenceScore,
      isDirect: touchpoint.isDirect,
      isSynthetic: true
    } as const;

    await persistAttribution(db, order, {
      journey: {
        tier: syntheticTier,
        touchpoints: [touchpoint],
        winner: touchpoint,
        confidenceScore,
        attributionReason: input.attributionReason,
        orderOccurredAtUtc: orderOccurredAt,
        normalizationFailures: []
      },
      candidateEvaluation: {
        orderOccurredAtUtc: orderOccurredAt,
        orderTimestampSource: order.processed_at
          ? 'processed_at'
          : order.created_at_shopify
            ? 'created_at_shopify'
            : 'ingested_at',
        deterministicFirstParty: [],
        shopifyHint: syntheticTier === 'deterministic_shopify_hint' ? [syntheticCandidate] : [],
        ga4Fallback: syntheticTier === 'ga4_fallback' ? [syntheticCandidate] : [],
        normalizationFailures: []
      }
    });

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
  collectDeterministicFirstPartyCandidates,
  extractAttributionCandidatesForOrder,
  preprocessAttributionSnapshot
};

export { loadAttributionPreprocessingSnapshot, preprocessAttributionOrders, preprocessAttributionSnapshot };
export { CANONICAL_ATTRIBUTION_TIERS, computeCanonicalAttributionTiers } from './canonical-tiers.js';
export {
  AttributionRunConcurrencyError,
  buildAttributionRunConfigHash,
  claimAttributionRuns,
  enqueueAttributionRun,
  getAttributionRun,
  markAttributionRunCompleted,
  markAttributionRunFailed,
  resumeAttributionRun,
  updateAttributionRunProgress
} from './run-store.js';
export { buildEmptyAttributionRunProgress, parseAttributionRunProgress } from './run-progress.js';
export { executeAttributionRun } from './run-executor.js';
export { processAttributionRuns } from './run-jobs.js';
