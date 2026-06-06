import { randomUUID } from 'node:crypto';

import {
  ATTRIBUTION_QA_PAYLOAD_SCHEMA_VERSION,
  normalizeAttributionQaPayloadV1,
  type AttributionCreditRecordV1,
  type AttributionExplainRecordV1,
  type AttributionQaCandidateGroup,
  type AttributionQaConfidenceLabel,
  type AttributionQaMatchSource,
  type AttributionQaNormalizationFailureV1,
  type AttributionQaPayloadV1,
  type AttributionResultRecordV1
} from '../../../packages/attribution-schema/index.js';
import {
  buildAttributionConfidenceLabel,
  buildOrderAttributionAuditRecord
} from './order-attribution-audit.js';
import type { AttributionCandidate, AttributionCandidateExtractionResult } from './candidate-extraction.js';
import type { AttributionCredit, AttributionExecutionResult, AttributionModel } from './engine.js';
import type { ResolvedAttributionTouchpoint, ResolvedJourney } from './resolver.js';

export type AttributionQaSnapshotOrder = {
  shopify_order_id: string;
  name?: string | null;
  currency_code: string | null;
  subtotal_price: string | null;
  total_price: string;
  processed_at: Date | null;
  created_at_shopify: Date | null;
  ingested_at: Date;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  shopify_customer_id?: string | null;
  email_hash: string | null;
  identity_journey_id: string | null;
  source_name: string | null;
  raw_payload?: unknown;
};

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function normalizeDecimalString(value: string | number | null | undefined, fallback = '0.00'): string {
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
    return value.toFixed(2);
  }

  const normalized = String(value ?? '').trim();
  return /^\d+(?:\.\d+)?$/.test(normalized) ? normalized : fallback;
}

function normalizeCurrencyCode(value: string | null | undefined): string {
  return normalizeNullableString(value)?.toUpperCase() ?? 'USD';
}

function normalizeClickIdType(value: string | null | undefined): AttributionCreditRecordV1['click_id_type'] {
  switch (value) {
    case 'gclid':
    case 'gbraid':
    case 'wbraid':
    case 'fbclid':
    case 'ttclid':
    case 'msclkid':
      return value;
    default:
      return null;
  }
}

function readOrderName(order: AttributionQaSnapshotOrder): string | null {
  const explicitName = normalizeNullableString(order.name);
  if (explicitName) {
    return explicitName;
  }

  if (order.raw_payload && typeof order.raw_payload === 'object' && !Array.isArray(order.raw_payload)) {
    const rawName = (order.raw_payload as Record<string, unknown>).name;
    return typeof rawName === 'string' ? normalizeNullableString(rawName) : null;
  }

  return null;
}

function resolveMatchSource(
  source: AttributionCandidate['ingestionSource'] | ResolvedAttributionTouchpoint['ingestionSource'] | null | undefined,
  attributionReason: string | null | undefined
): AttributionQaMatchSource {
  if (source === 'customer_identity') {
    return attributionReason === 'matched_by_identity_journey' ? 'stitched_identity_journey' : 'customer_identity';
  }

  if (source === 'shopify_marketing_hint') {
    return 'shopify_marketing_hint';
  }

  if (source === 'meta_platform_reported') {
    return 'meta_platform_reported';
  }

  if (source === 'ga4_fallback') {
    return 'ga4_fallback';
  }

  if (source === 'landing_session_id' || source === 'checkout_token' || source === 'cart_token') {
    return source;
  }

  return 'unattributed';
}

function confidenceLabel(score: number): AttributionQaConfidenceLabel {
  return score <= 0 ? 'none' : buildAttributionConfidenceLabel(score);
}

function candidateGroup(sourceClass: AttributionCandidate['sourceClass']): AttributionQaCandidateGroup {
  return sourceClass === 'deterministic_shopify_hint' ? 'shopify_hint' : sourceClass;
}

function candidateTouchpointId(candidate: AttributionCandidate): string {
  return candidate.sourceTouchEventId ?? candidate.sessionId ?? candidate.sourceKey;
}

function isSelectedCandidate(candidate: AttributionCandidate, journey: ResolvedJourney): boolean {
  if (!journey.winner) {
    return false;
  }

  return (
    candidate.ingestionSource === journey.winner.ingestionSource &&
    candidate.occurredAtUtc.getTime() === journey.winner.occurredAt.getTime() &&
    candidate.sessionId === journey.winner.sessionId &&
    candidate.sourceTouchEventId === journey.winner.sourceTouchEventId &&
    candidate.attributionReason === journey.winner.attributionReason
  );
}

function mapCandidate(candidate: AttributionCandidate, journey: ResolvedJourney) {
  return {
    candidate_group: candidateGroup(candidate.sourceClass),
    source_key: candidate.sourceKey,
    touchpoint_id: candidateTouchpointId(candidate),
    session_id: candidate.sessionId,
    source_touch_event_id: candidate.sourceTouchEventId,
    occurred_at_utc: candidate.occurredAtUtc.toISOString(),
    source: candidate.source,
    medium: candidate.medium,
    campaign: candidate.campaign,
    content: candidate.content,
    term: candidate.term,
    click_id_type: candidate.clickIdType,
    click_id_value: candidate.clickIdValue,
    match_source: resolveMatchSource(candidate.ingestionSource, candidate.attributionReason),
    attribution_reason: candidate.attributionReason,
    confidence_score: candidate.confidenceScore,
    confidence_label: confidenceLabel(candidate.confidenceScore),
    is_direct: candidate.isDirect,
    is_synthetic: candidate.isSynthetic,
    selected: isSelectedCandidate(candidate, journey)
  };
}

function flattenCandidates(candidates: AttributionCandidateExtractionResult): AttributionCandidate[] {
  return [
    ...candidates.deterministicFirstParty,
    ...candidates.shopifyHint,
    ...(candidates.platformReportedMeta ?? []),
    ...candidates.ga4Fallback
  ];
}

function candidateDecisionReason(candidate: AttributionCandidate, journey: ResolvedJourney): string {
  if (isSelectedCandidate(candidate, journey)) {
    return `selected_${journey.tier}`;
  }

  if (!journey.winner) {
    return 'no_winner_selected';
  }

  if (candidate.sourceClass !== 'deterministic_first_party' && journey.tier === 'deterministic_first_party') {
    return 'blocked_by_deterministic_first_party_winner';
  }

  if (candidate.sourceClass === 'ga4_fallback' && journey.tier === 'deterministic_shopify_hint') {
    return 'blocked_by_shopify_hint_winner';
  }

  if (candidate.occurredAtUtc.getTime() > (journey.orderOccurredAtUtc?.getTime() ?? Number.POSITIVE_INFINITY)) {
    return 'candidate_after_order_timestamp';
  }

  return 'lower_priority_than_selected_winner';
}

function buildMissingFieldDiagnostics(order: AttributionQaSnapshotOrder): AttributionQaNormalizationFailureV1[] {
  const fields: Array<[string, string | null | undefined]> = [
    ['landing_session_id', order.landing_session_id],
    ['checkout_token', order.checkout_token],
    ['cart_token', order.cart_token],
    ['shopify_customer_id', order.shopify_customer_id],
    ['email_hash', order.email_hash],
    ['identity_journey_id', order.identity_journey_id]
  ];

  return fields
    .filter(([, value]) => !normalizeNullableString(value))
    .map(([field]) => ({
      scope: 'order',
      reason: `missing_${field}`,
      source_key: order.shopify_order_id
    }));
}

function buildDiagnosticsNotes(
  order: AttributionQaSnapshotOrder,
  candidates: AttributionCandidateExtractionResult,
  journey: ResolvedJourney
): string[] {
  const notes = [
    `candidate_counts deterministic_first_party=${candidates.deterministicFirstParty.length} shopify_hint=${candidates.shopifyHint.length} platform_reported_meta=${(candidates.platformReportedMeta ?? []).length} ga4_fallback=${candidates.ga4Fallback.length}`,
    journey.winner
      ? `winner selected from ${journey.tier} with reason ${journey.attributionReason}`
      : `no winner selected with reason ${journey.attributionReason}`
  ];

  const missingFields = buildMissingFieldDiagnostics(order).map((failure) => failure.reason.replace(/^missing_/, ''));
  if (missingFields.length > 0) {
    notes.push(`missing order join fields: ${missingFields.join(', ')}`);
  }

  notes.push(
    candidates.ga4Fallback.length > 0
      ? `ga4 fallback evaluated ${candidates.ga4Fallback.length} candidate(s)`
      : 'ga4 fallback evaluated no candidates'
  );

  return notes;
}

function buildModelSummaries(
  runId: string,
  orderId: string,
  generatedAt: Date,
  execution: AttributionExecutionResult
): AttributionResultRecordV1[] {
  return execution.models.map((model) => {
    const summary = execution.summariesByModel[model];
    return {
      run_id: runId,
      attribution_spec_version: 'v1',
      order_id: orderId,
      model_key: summary.attributionModel,
      allocation_status: summary.allocationStatus,
      winner_touchpoint_id: summary.winnerTouchpointId,
      winner_session_id: summary.winnerSessionId,
      winner_evidence_source: summary.winnerEvidenceSource,
      winner_attribution_reason: summary.winnerAttributionReason,
      total_credit_weight: summary.totalCreditWeight.toFixed(2),
      total_revenue_credited: summary.totalRevenueCredited,
      touchpoint_count_considered: summary.touchpointCountConsidered,
      eligible_click_count: summary.eligibleClickCount,
      eligible_view_count: summary.eligibleViewCount,
      lookback_rule_applied: summary.lookbackRuleApplied,
      winner_selection_rule: summary.winnerSelectionRule,
      direct_suppression_applied: summary.directSuppressionApplied,
      deterministic_block_applied: summary.deterministicBlockApplied,
      normalization_failures_count: summary.normalizationFailuresCount,
      generated_at_utc: generatedAt.toISOString()
    };
  });
}

function buildCredits(runId: string, orderId: string, outputs: AttributionExecutionResult): AttributionCreditRecordV1[] {
  return outputs.models.flatMap((model) =>
    outputs.creditsByModel[model].map((credit: AttributionCredit) => ({
      run_id: runId,
      attribution_spec_version: 'v1',
      order_id: orderId,
      model_key: credit.attributionModel,
      touchpoint_id: credit.touchpointId ?? `${credit.attributionModel}:${credit.touchpointPosition}`,
      session_id: credit.sessionId,
      touchpoint_position: credit.touchpointPosition + 1,
      occurred_at_utc: credit.touchpointOccurredAt.toISOString(),
      source: credit.source,
      medium: credit.medium,
      campaign: credit.campaign,
      content: credit.content,
      term: credit.term,
      click_id_type: normalizeClickIdType(credit.clickIdType),
      click_id_value: credit.clickIdValue,
      touch_type: credit.engagementType === 'view' ? 'view' : 'click',
      is_direct: credit.isDirect,
      evidence_source: credit.evidenceSource ?? 'customer_identity',
      is_synthetic: credit.isSynthetic,
      attribution_reason: credit.attributionReason,
      credit_weight: credit.creditWeight.toFixed(2),
      revenue_credit: credit.revenueCredit,
      is_primary: credit.isPrimary
    }))
  );
}

function selectedModel(outputs: AttributionExecutionResult): AttributionModel {
  return outputs.creditsByModel.last_non_direct.some((credit) => credit.isPrimary)
    ? 'last_non_direct'
    : 'hinted_fallback_only';
}

function buildExplainability(
  runId: string,
  orderId: string,
  generatedAt: Date,
  candidates: AttributionCandidateExtractionResult,
  journey: ResolvedJourney,
  outputs: AttributionExecutionResult
): AttributionExplainRecordV1[] {
  const orderOccurredAt = journey.orderOccurredAtUtc?.toISOString() ?? null;
  const records: AttributionExplainRecordV1[] = flattenCandidates(candidates).map((candidate) => {
    const selected = isSelectedCandidate(candidate, journey);
    return {
      run_id: runId,
      order_id: orderId,
      touchpoint_id: candidateTouchpointId(candidate),
      model_key: selected ? selectedModel(outputs) : null,
      explain_stage: selected ? 'model_scoring' : 'eligibility_filter',
      decision: selected ? 'winner' : 'excluded',
      decision_reason: candidateDecisionReason(candidate, journey),
      details_json: {
        candidate_group: candidateGroup(candidate.sourceClass),
        match_source: resolveMatchSource(candidate.ingestionSource, candidate.attributionReason),
        confidence_score: candidate.confidenceScore,
        is_direct: candidate.isDirect,
        is_synthetic: candidate.isSynthetic,
        winner_tier: journey.tier,
        winner_reason: journey.attributionReason
      },
      order_occurred_at_utc: orderOccurredAt,
      created_at_utc: generatedAt.toISOString()
    };
  });

  records.push({
    run_id: runId,
    order_id: orderId,
    touchpoint_id: null,
    model_key: null,
    explain_stage: 'fallback',
    decision:
      journey.tier === 'ga4_fallback'
        ? 'fallback_used'
        : journey.winner
          ? 'excluded'
          : 'no_credit',
    decision_reason:
      journey.tier === 'ga4_fallback'
        ? 'ga4_fallback_selected_after_no_higher_tier_winner'
        : journey.winner
          ? `ga4_fallback_not_used_${journey.tier}_winner_selected`
          : 'ga4_fallback_no_eligible_candidate',
    details_json: {
      deterministic_first_party_count: candidates.deterministicFirstParty.length,
      shopify_hint_count: candidates.shopifyHint.length,
      ga4_fallback_count: candidates.ga4Fallback.length,
      normalization_failures: candidates.normalizationFailures
    },
    order_occurred_at_utc: orderOccurredAt,
    created_at_utc: generatedAt.toISOString()
  });

  if (records.length === 1 && !journey.winner) {
    records.push({
      run_id: runId,
      order_id: orderId,
      touchpoint_id: null,
      model_key: 'last_non_direct',
      explain_stage: 'candidate_extraction',
      decision: 'no_credit',
      decision_reason: journey.attributionReason,
      details_json: {
        deterministic_first_party_count: candidates.deterministicFirstParty.length,
        shopify_hint_count: candidates.shopifyHint.length,
        ga4_fallback_count: candidates.ga4Fallback.length
      },
      order_occurred_at_utc: orderOccurredAt,
      created_at_utc: generatedAt.toISOString()
    });
  }

  return records;
}

export function buildAttributionQaSnapshot(input: {
  order: AttributionQaSnapshotOrder;
  candidates: AttributionCandidateExtractionResult;
  journey: ResolvedJourney;
  execution: AttributionExecutionResult;
  generatedAt: Date;
}): AttributionQaPayloadV1 {
  const runId = randomUUID();
  const audit = buildOrderAttributionAuditRecord(input.journey, input.generatedAt);
  const allNormalizationFailures = [
    ...input.candidates.normalizationFailures.map((failure) => ({
      scope: failure.scope,
      reason: failure.reason,
      source_key: failure.sourceKey
    })),
    ...buildMissingFieldDiagnostics(input.order)
  ];

  return normalizeAttributionQaPayloadV1({
    schema_version: ATTRIBUTION_QA_PAYLOAD_SCHEMA_VERSION,
    generated_at_utc: input.generatedAt.toISOString(),
    order: {
      order_id: input.order.shopify_order_id,
      order_platform: 'shopify',
      order_name: readOrderName(input.order),
      order_occurred_at_utc: input.candidates.orderOccurredAtUtc?.toISOString() ?? null,
      order_timestamp_source: input.candidates.orderTimestampSource,
      currency_code: normalizeCurrencyCode(input.order.currency_code),
      subtotal_amount: normalizeDecimalString(input.order.subtotal_price, normalizeDecimalString(input.order.total_price)),
      total_amount: normalizeDecimalString(input.order.total_price),
      source_name: input.order.source_name,
      identifiers: {
        landing_session_id: input.order.landing_session_id,
        checkout_token: input.order.checkout_token,
        cart_token: input.order.cart_token,
        shopify_customer_id: input.order.shopify_customer_id ?? null,
        email_hash: input.order.email_hash,
        identity_journey_id: input.order.identity_journey_id
      }
    },
    outcome: {
      status: input.journey.winner ? 'success' : 'no_match',
      attribution_tier: input.journey.tier,
      attribution_reason: input.journey.attributionReason,
      match_source: resolveMatchSource(input.journey.winner?.ingestionSource, input.journey.attributionReason),
      confidence_score: input.journey.confidenceScore,
      confidence_label: confidenceLabel(input.journey.confidenceScore),
      winner_touchpoint_id: input.journey.winner
        ? input.journey.winner.sourceTouchEventId ?? input.journey.winner.sessionId ?? input.journey.attributionReason
        : null,
      winner_session_id: input.journey.winner?.sessionId ?? null,
      selected_model_key: selectedModel(input.execution)
    },
    candidates: {
      deterministic_first_party: input.candidates.deterministicFirstParty.map((candidate) =>
        mapCandidate(candidate, input.journey)
      ),
      shopify_hint: input.candidates.shopifyHint.map((candidate) => mapCandidate(candidate, input.journey)),
      platform_reported_meta: (input.candidates.platformReportedMeta ?? []).map((candidate) =>
        mapCandidate(candidate, input.journey)
      ),
      ga4_fallback: input.candidates.ga4Fallback.map((candidate) => mapCandidate(candidate, input.journey))
    },
    model_summaries: buildModelSummaries(runId, input.order.shopify_order_id, input.generatedAt, input.execution),
    credits: buildCredits(runId, input.order.shopify_order_id, input.execution),
    explainability: buildExplainability(
      runId,
      input.order.shopify_order_id,
      input.generatedAt,
      input.candidates,
      input.journey,
      input.execution
    ),
    diagnostics: {
      normalization_failures: allNormalizationFailures,
      notes: buildDiagnosticsNotes(input.order, input.candidates, input.journey)
    }
  });
}
