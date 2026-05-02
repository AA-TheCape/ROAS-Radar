import { createHash } from 'node:crypto';

import type { PoolClient } from 'pg';

import type { OrderAttributionAuditRecord } from './order-attribution-audit.js';
import type {
  ResolvedJourney,
  ResolvedAttributionTier,
  TieredAttributionCandidate,
  TieredAttributionResolverInput
} from './resolver.js';

type DecisionArtifactOrderInput = {
  shopifyOrderId: string;
  payloadHash: string | null;
  attributionTier: ResolvedAttributionTier | null;
};

type DecisionArtifactPersistenceInput = {
  client: PoolClient;
  order: DecisionArtifactOrderInput;
  journey: ResolvedJourney;
  resolverInput: TieredAttributionResolverInput;
  orderAttributionAudit: OrderAttributionAuditRecord;
  resolverRunSource: 'forward_processing' | 'manual_backfill';
  resolverTriggeredBy: string;
  backfillRunId?: string | null;
};

type SerializedResolverCandidate = {
  sourceKey: string;
  sessionId: string | null;
  sourceTouchEventId: string | null;
  ingestionSource: string;
  occurredAtUtc: string;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  confidenceScore: number;
  isDirect: boolean;
  isSynthetic: boolean;
  metaSignalId: string | null;
  metaMatchBasis: string | null;
  metaEligibilityOutcome: string | null;
  isClickThrough: boolean | null;
  isViewThrough: boolean | null;
};

function normalizeResolverTriggeredBy(value: string): string {
  const normalized = value.trim();
  return normalized.length > 0 ? normalized.slice(0, 255) : 'unspecified';
}

function stableSerialize(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableSerialize(entry)).join(',')}]`;
  }

  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableSerialize(record[key])}`).join(',')}}`;
}

function hashStablePayload(value: unknown): string {
  return createHash('sha256').update(stableSerialize(value)).digest('hex');
}

function serializeResolverCandidate(candidate: TieredAttributionCandidate): SerializedResolverCandidate {
  return {
    sourceKey: candidate.sourceKey,
    sessionId: candidate.sessionId,
    sourceTouchEventId: candidate.sourceTouchEventId,
    ingestionSource: candidate.ingestionSource,
    occurredAtUtc: candidate.occurredAtUtc.toISOString(),
    source: candidate.source,
    medium: candidate.medium,
    campaign: candidate.campaign,
    content: candidate.content,
    term: candidate.term,
    clickIdType: candidate.clickIdType,
    clickIdValue: candidate.clickIdValue,
    attributionReason: candidate.attributionReason,
    confidenceScore: candidate.confidenceScore,
    isDirect: candidate.isDirect,
    isSynthetic: candidate.isSynthetic,
    metaSignalId: candidate.metaSignalId ?? null,
    metaMatchBasis: candidate.metaMatchBasis ?? null,
    metaEligibilityOutcome: candidate.metaEligibilityOutcome ?? null,
    isClickThrough: candidate.isClickThrough ?? null,
    isViewThrough: candidate.isViewThrough ?? null
  };
}

export function buildResolverInputHash(input: TieredAttributionResolverInput, resolverRuleVersion: string): string {
  return hashStablePayload({
    resolverRuleVersion,
    orderOccurredAtUtc: input.orderOccurredAtUtc?.toISOString() ?? null,
    deterministicFirstParty: input.deterministicFirstParty.map(serializeResolverCandidate),
    shopifyHint: input.shopifyHint.map(serializeResolverCandidate),
    platformReportedMeta: (input.platformReportedMeta ?? []).map(serializeResolverCandidate),
    ga4Fallback: input.ga4Fallback.map(serializeResolverCandidate),
    normalizationFailures: (input.normalizationFailures ?? []).map((failure) => ({
      scope: failure.scope,
      reason: failure.reason,
      sourceKey: failure.sourceKey
    }))
  });
}

export async function insertAttributionDecisionArtifact(
  input: DecisionArtifactPersistenceInput
): Promise<string> {
  const hasMetaEvidence = (input.resolverInput.platformReportedMeta?.length ?? 0) > 0;
  const hasHigherPrecedenceWinner =
    input.journey.tier === 'deterministic_first_party' || input.journey.tier === 'deterministic_shopify_hint';
  const metaEvaluationOutcome =
    input.journey.tier === 'platform_reported_meta'
      ? 'eligible_canonical'
      : 'not_evaluated';
  const decisionReason =
    input.journey.tier === 'platform_reported_meta'
      ? 'meta_canonical_selected'
      : hasMetaEvidence && hasHigherPrecedenceWinner
        ? 'meta_not_evaluated_higher_precedence_winner'
        : 'meta_not_evaluated_no_evidence';
  const result = await input.client.query<{ id: string }>(
    `
      INSERT INTO attribution_decision_artifacts (
        shopify_order_id,
        meta_attribution_evidence_id,
        backfill_run_id,
        resolver_run_source,
        resolver_triggered_by,
        resolver_rule_version,
        resolver_model_version,
        canonical_tier_before,
        canonical_tier_after,
        meta_evaluation_outcome,
        meta_affected_canonical,
        decision_reason,
        decision_reason_detail,
        confidence_score,
        confidence_threshold,
        rule_inputs_hash,
        evidence_snapshot_hash,
        order_occurred_at_utc,
        order_snapshot_ref,
        first_party_winner_present,
        shopify_hint_winner_present,
        ga4_fallback_candidate_present,
        canonical_winner_tier,
        canonical_winner_source,
        parallel_meta_available,
        replayable
      )
      VALUES (
        $1,
        NULL,
        $2,
        $3,
        $4,
        $5,
        1,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        NULL,
        $15,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21,
        $22,
        true
      )
      RETURNING id::text
    `,
    [
      input.order.shopifyOrderId,
      input.backfillRunId ?? null,
      input.resolverRunSource,
      normalizeResolverTriggeredBy(input.resolverTriggeredBy),
      input.journey.resolverRuleVersion,
      input.order.attributionTier ?? 'unattributed',
      input.journey.tier,
      metaEvaluationOutcome,
      input.journey.tier === 'platform_reported_meta',
      decisionReason,
      hasMetaEvidence ? 'Meta evidence versioning is not fully wired into this resolver path yet.' : null,
      input.journey.tier === 'platform_reported_meta' ? input.journey.confidenceScore : null,
      input.journey.tier === 'platform_reported_meta' ? 0.5 : null,
      buildResolverInputHash(input.resolverInput, input.journey.resolverRuleVersion),
      input.journey.orderOccurredAtUtc ?? null,
      input.order.payloadHash
        ? `shopify_orders:${input.order.shopifyOrderId}:payload_hash:${input.order.payloadHash}`
        : `shopify_orders:${input.order.shopifyOrderId}`,
      input.resolverInput.deterministicFirstParty.length > 0,
      input.resolverInput.shopifyHint.length > 0,
      input.resolverInput.ga4Fallback.length > 0,
      input.journey.tier,
      input.orderAttributionAudit.source,
      hasMetaEvidence && input.journey.tier !== 'platform_reported_meta'
    ]
  );

  return result.rows[0].id;
}
