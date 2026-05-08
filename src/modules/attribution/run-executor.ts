import type { PoolClient } from 'pg';

import { withTransaction } from '../../db/pool.js';
import { ATTRIBUTION_MODELS, executeAttributionModels, type AttributionTouchpoint } from './engine.js';
import { preprocessAttributionOrders, type AttributionPreprocessingDataset } from './preprocessing.js';
import { parseAttributionRunProgress, type AttributionRunProgress } from './run-progress.js';
import type { ClaimedAttributionRun } from './run-store.js';

type ExecuteAttributionRunOptions = {
  run: ClaimedAttributionRun;
  now?: Date;
  onProgress?: (progress: AttributionRunProgress) => Promise<void> | void;
};

type ExecuteAttributionRunReport = {
  runId: string;
  inputSnapshotHash: string;
  orderCount: number;
  processedOrders: number;
  succeededOrders: number;
  failedOrders: number;
  batchesProcessed: number;
  retryOrderIdsOutstanding: string[];
  lastProcessedOrderId: string | null;
};

type ExplainDecision = 'included' | 'excluded' | 'winner' | 'fallback_used' | 'no_credit';
type ExplainStage = 'candidate_extraction' | 'eligibility_filter' | 'model_scoring' | 'fallback';

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function buildEngineTouchpoint(datasetTouchpoint: AttributionPreprocessingDataset['touchpoints'][number]): AttributionTouchpoint {
  return {
    touchpointId: datasetTouchpoint.touchpoint_id,
    sessionId: datasetTouchpoint.session_id,
    occurredAt: new Date(datasetTouchpoint.touchpoint_occurred_at_utc),
    source: datasetTouchpoint.source,
    medium: datasetTouchpoint.medium,
    campaign: datasetTouchpoint.campaign,
    content: datasetTouchpoint.content,
    term: datasetTouchpoint.term,
    clickIdType: datasetTouchpoint.click_id_type,
    clickIdValue: datasetTouchpoint.click_id_value,
    attributionReason: datasetTouchpoint.attribution_reason ?? 'unknown',
    isDirect: datasetTouchpoint.is_direct,
    isForced: datasetTouchpoint.is_synthetic,
    sourceTouchEventId: datasetTouchpoint.touchpoint_id,
    ingestionSource: datasetTouchpoint.ingestion_source,
    evidenceSource: datasetTouchpoint.evidence_source,
    engagementType: datasetTouchpoint.engagement_type,
    isSynthetic: datasetTouchpoint.is_synthetic
  };
}

function buildConfidenceLabel(touchpoint: AttributionPreprocessingDataset['touchpoints'][number]): 'none' | 'low' | 'medium' | 'high' {
  if (touchpoint.evidence_source === 'shopify_marketing_hint') {
    return touchpoint.click_id_value ? 'medium' : 'low';
  }

  if (touchpoint.evidence_source === 'ga4_fallback') {
    return 'low';
  }

  return 'high';
}

async function insertExplainRecord(
  client: PoolClient,
  input: {
    runId: string;
    orderId: string;
    touchpointId?: string | null;
    modelKey?: string | null;
    stage: ExplainStage;
    decision: ExplainDecision;
    reason: string;
    details: Record<string, unknown>;
    orderOccurredAtUtc?: string | null;
  }
): Promise<void> {
  await client.query(
    `
      INSERT INTO attribution_explain_records (
        run_id,
        order_id,
        touchpoint_id,
        model_key,
        explain_stage,
        decision,
        decision_reason,
        details_json,
        order_occurred_at_utc
      )
      VALUES (
        $1::uuid,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8::jsonb,
        $9::timestamptz
      )
    `,
    [
      input.runId,
      input.orderId,
      input.touchpointId ?? null,
      input.modelKey ?? null,
      input.stage,
      input.decision,
      input.reason,
      JSON.stringify(input.details),
      input.orderOccurredAtUtc ?? null
    ]
  );
}

async function persistBatch(
  client: PoolClient,
  run: ClaimedAttributionRun,
  orderIds: string[]
): Promise<{ succeededOrderIds: string[]; failedOrderIds: string[] }> {
  const succeededOrderIds: string[] = [];
  const failedOrderIds: string[] = [];

  for (const orderId of orderIds) {
    const dataset = await preprocessAttributionOrders(client, [orderId]);
    const order = dataset.orders[0];
    if (!order) {
      failedOrderIds.push(orderId);
      continue;
    }

    const orderTouchpoints = dataset.touchpoints;
    const orderFailures = dataset.failures.filter((failure) => failure.orderId === orderId).map((failure) => failure.reasonCode);

    await client.query('DELETE FROM attribution_order_inputs WHERE run_id = $1::uuid AND order_id = $2', [run.id, orderId]);

    await client.query(
      `
        INSERT INTO attribution_order_inputs (
          run_id,
          schema_version,
          order_id,
          order_platform,
          order_occurred_at_utc,
          order_timestamp_source,
          currency_code,
          subtotal_amount,
          total_amount,
          landing_session_id,
          checkout_token,
          cart_token,
          shopify_customer_id,
          email_hash,
          source_name,
          identity_journey_id,
          raw_order_ref
        )
        VALUES (
          $1::uuid,
          $2,
          $3,
          $4,
          $5::timestamptz,
          $6,
          $7,
          $8::numeric,
          $9::numeric,
          $10::uuid,
          $11,
          $12,
          $13,
          $14,
          $15,
          $16::uuid,
          $17::jsonb
        )
      `,
      [
        run.id,
        order.schema_version,
        order.order_id,
        order.order_platform,
        order.order_occurred_at_utc,
        order.order_timestamp_source,
        order.currency_code,
        order.subtotal_amount,
        order.total_amount,
        order.landing_session_id,
        order.checkout_token,
        order.cart_token,
        order.shopify_customer_id,
        order.email_hash,
        order.source_name,
        order.identity_journey_id,
        JSON.stringify(order.raw_order_ref ?? {})
      ]
    );

    for (const touchpoint of orderTouchpoints) {
      await client.query(
        `
          INSERT INTO attribution_touchpoint_inputs (
            run_id,
            order_id,
            schema_version,
            touchpoint_id,
            session_id,
            identity_journey_id,
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
          )
          VALUES (
            $1::uuid,
            $2,
            $3,
            $4,
            $5::uuid,
            $6::uuid,
            $7::timestamptz,
            $8::timestamptz,
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
            $25::jsonb
          )
        `,
        [
          run.id,
          orderId,
          touchpoint.schema_version,
          touchpoint.touchpoint_id,
          touchpoint.session_id,
          touchpoint.identity_journey_id,
          touchpoint.touchpoint_occurred_at_utc,
          touchpoint.touchpoint_captured_at_utc,
          touchpoint.touchpoint_source_kind,
          touchpoint.ingestion_source,
          touchpoint.source,
          touchpoint.medium,
          touchpoint.campaign,
          touchpoint.content,
          touchpoint.term,
          touchpoint.click_id_type,
          touchpoint.click_id_value,
          touchpoint.evidence_source,
          touchpoint.is_direct,
          touchpoint.engagement_type,
          touchpoint.is_synthetic,
          touchpoint.is_eligible,
          touchpoint.ineligibility_reason,
          touchpoint.attribution_reason,
          JSON.stringify(touchpoint.attribution_hint ?? {})
        ]
      );

      await insertExplainRecord(client, {
        runId: run.id,
        orderId,
        touchpointId: touchpoint.touchpoint_id,
        stage: 'candidate_extraction',
        decision: 'included',
        reason: 'touchpoint_normalized',
        details: {
          evidenceSource: touchpoint.evidence_source,
          touchType: touchpoint.engagement_type,
          synthetic: touchpoint.is_synthetic
        },
        orderOccurredAtUtc: order.order_occurred_at_utc
      });

      await insertExplainRecord(client, {
        runId: run.id,
        orderId,
        touchpointId: touchpoint.touchpoint_id,
        stage: 'eligibility_filter',
        decision: touchpoint.is_eligible ? 'included' : 'excluded',
        reason: touchpoint.is_eligible ? 'within_lookback_window' : touchpoint.ineligibility_reason ?? 'ineligible',
        details: {
          evidenceSource: touchpoint.evidence_source,
          touchType: touchpoint.engagement_type
        },
        orderOccurredAtUtc: order.order_occurred_at_utc
      });
    }

    const execution = executeAttributionModels(
      orderTouchpoints.map((touchpoint) => buildEngineTouchpoint(touchpoint)),
      {
        orderOccurredAt: new Date(order.order_occurred_at_utc),
        orderRevenue: order.total_amount,
        attributionModels: ATTRIBUTION_MODELS,
        normalizationFailuresCount: orderFailures.length
      }
    );

    for (const model of ATTRIBUTION_MODELS) {
      const summary = execution.summariesByModel[model];

      await client.query(
        `
          INSERT INTO attribution_model_summaries (
            run_id,
            attribution_spec_version,
            order_id,
            model_key,
            allocation_status,
            winner_touchpoint_id,
            winner_session_id,
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
            normalization_failures_count,
            order_occurred_at_utc
          )
          VALUES (
            $1::uuid,
            'v1',
            $2,
            $3,
            $4,
            $5,
            $6::uuid,
            $7,
            $8,
            $9::numeric,
            $10::numeric,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16,
            $17,
            $18,
            $19::timestamptz
          )
        `,
        [
          run.id,
          orderId,
          model,
          summary.allocationStatus,
          summary.winnerTouchpointId,
          summary.winnerSessionId,
          summary.winnerEvidenceSource,
          summary.winnerAttributionReason,
          summary.totalCreditWeight,
          summary.totalRevenueCredited,
          summary.touchpointCountConsidered,
          summary.eligibleClickCount,
          summary.eligibleViewCount,
          summary.lookbackRuleApplied,
          summary.winnerSelectionRule,
          summary.directSuppressionApplied,
          summary.deterministicBlockApplied,
          summary.normalizationFailuresCount,
          order.order_occurred_at_utc
        ]
      );

      const modelCredits = execution.creditsByModel[model];
      const creditedTouchpointIds = new Set(modelCredits.map((credit) => credit.touchpointId).filter(Boolean));

      for (const credit of modelCredits) {
        await client.query(
          `
            INSERT INTO attribution_model_credits (
              run_id,
              order_id,
              model_key,
              attribution_spec_version,
              touchpoint_id,
              session_id,
              touchpoint_position,
              occurred_at_utc,
              source,
              medium,
              campaign,
              content,
              term,
              click_id_type,
              click_id_value,
              touch_type,
              is_direct,
              evidence_source,
              is_synthetic,
              attribution_reason,
              credit_weight,
              revenue_credit,
              is_primary,
              match_source,
              confidence_label
            )
            VALUES (
              $1::uuid,
              $2,
              $3,
              'v1',
              $4,
              $5::uuid,
              $6,
              $7::timestamptz,
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
              $20::numeric,
              $21::numeric,
              $22,
              $23,
              $24
            )
          `,
          [
            run.id,
            orderId,
            model,
            credit.touchpointId,
            credit.sessionId,
            credit.touchpointPosition,
            credit.touchpointOccurredAt.toISOString(),
            credit.source,
            credit.medium,
            credit.campaign,
            credit.content,
            credit.term,
            credit.clickIdType,
            credit.clickIdValue,
            credit.engagementType,
            credit.isDirect,
            credit.evidenceSource,
            credit.isSynthetic,
            credit.attributionReason,
            credit.creditWeight,
            credit.revenueCredit,
            credit.isPrimary,
            credit.evidenceSource,
            buildConfidenceLabel(
              orderTouchpoints.find((touchpoint) => touchpoint.touchpoint_id === credit.touchpointId) ?? orderTouchpoints[0]
            )
          ]
        );

        await insertExplainRecord(client, {
          runId: run.id,
          orderId,
          touchpointId: credit.touchpointId,
          modelKey: model,
          stage: model === 'hinted_fallback_only' ? 'fallback' : 'model_scoring',
          decision: credit.isPrimary ? 'winner' : 'included',
          reason: credit.attributionReason,
          details: {
            creditWeight: credit.creditWeight,
            revenueCredit: credit.revenueCredit
          },
          orderOccurredAtUtc: order.order_occurred_at_utc
        });
      }

      for (const touchpoint of orderTouchpoints) {
        if (creditedTouchpointIds.has(touchpoint.touchpoint_id)) {
          continue;
        }

        await insertExplainRecord(client, {
          runId: run.id,
          orderId,
          touchpointId: touchpoint.touchpoint_id,
          modelKey: model,
          stage: model === 'hinted_fallback_only' ? 'fallback' : 'model_scoring',
          decision: 'no_credit',
          reason:
            summary.allocationStatus === 'blocked_by_deterministic'
              ? 'blocked_by_deterministic'
              : summary.allocationStatus === 'no_eligible_touches'
                ? 'no_eligible_touches'
                : 'not_selected_by_model',
          details: {
            allocationStatus: summary.allocationStatus
          },
          orderOccurredAtUtc: order.order_occurred_at_utc
        });
      }
    }

    succeededOrderIds.push(orderId);
  }

  return {
    succeededOrderIds,
    failedOrderIds
  };
}

export async function executeAttributionRun(options: ExecuteAttributionRunOptions): Promise<ExecuteAttributionRunReport> {
  const run = options.run;
  const progress = parseAttributionRunProgress(run.progress);
  const snapshotOrderIds = run.inputSnapshot.orderIds;
  const publishProgress = options.onProgress
    ? async () => options.onProgress?.(parseAttributionRunProgress(progress))
    : null;

  while (progress.cursor.offset < snapshotOrderIds.length) {
    const batchOrderIds = snapshotOrderIds.slice(progress.cursor.offset, progress.cursor.offset + run.batchSize);
    const batchResult = await withTransaction((client) => persistBatch(client, run, batchOrderIds));

    progress.processedOrders += batchOrderIds.length;
    progress.succeededOrders += batchResult.succeededOrderIds.length;
    progress.failedOrders += batchResult.failedOrderIds.length;
    progress.retryOrderIds = Array.from(new Set([...progress.retryOrderIds, ...batchResult.failedOrderIds])).sort();
    progress.lastProcessedOrderId = batchOrderIds[batchOrderIds.length - 1] ?? progress.lastProcessedOrderId;
    progress.cursor.offset += batchOrderIds.length;
    progress.cursor.batchesProcessed += 1;
    progress.cursor.completed = progress.cursor.offset >= snapshotOrderIds.length;

    if (publishProgress) {
      await publishProgress();
    }
  }

  if (progress.retryOrderIds.length > 0) {
    const retryBatch = progress.retryOrderIds.slice(0, run.batchSize);
    const retryResult = await withTransaction((client) => persistBatch(client, run, retryBatch));
    const succeededRetrySet = new Set(retryResult.succeededOrderIds);

    progress.retryOrderIds = progress.retryOrderIds.filter((orderId) => !succeededRetrySet.has(orderId));
    progress.succeededOrders += retryResult.succeededOrderIds.length;
    progress.failedOrders += retryResult.failedOrderIds.length;
    progress.processedOrders += retryBatch.length;
    progress.lastProcessedOrderId = retryBatch[retryBatch.length - 1] ?? progress.lastProcessedOrderId;

    if (publishProgress) {
      await publishProgress();
    }
  }

  return {
    runId: run.id,
    inputSnapshotHash: run.inputSnapshotHash,
    orderCount: snapshotOrderIds.length,
    processedOrders: progress.processedOrders,
    succeededOrders: progress.succeededOrders,
    failedOrders: progress.failedOrders,
    batchesProcessed: progress.cursor.batchesProcessed,
    retryOrderIdsOutstanding: progress.retryOrderIds,
    lastProcessedOrderId: progress.lastProcessedOrderId
  };
}
