import { createHash } from 'node:crypto';
import { summarizeMetaAttribution } from './meta-evaluation.js';
function normalizeResolverTriggeredBy(value) {
    const normalized = value.trim();
    return normalized.length > 0 ? normalized.slice(0, 255) : 'unspecified';
}
function stableSerialize(value) {
    if (value === null || typeof value !== 'object') {
        return JSON.stringify(value);
    }
    if (Array.isArray(value)) {
        return `[${value.map((entry) => stableSerialize(entry)).join(',')}]`;
    }
    const record = value;
    const keys = Object.keys(record).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableSerialize(record[key])}`).join(',')}}`;
}
function hashStablePayload(value) {
    return createHash('sha256').update(stableSerialize(value)).digest('hex');
}
function serializeResolverCandidate(candidate) {
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
export function buildResolverInputHash(input, resolverRuleVersion) {
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
export async function insertAttributionDecisionArtifact(input) {
    const hasMetaEvidence = (input.resolverInput.platformReportedMeta?.length ?? 0) > 0;
    const hasHigherPrecedenceWinner = input.journey.tier === 'deterministic_first_party' || input.journey.tier === 'deterministic_shopify_hint';
    const metaSummary = summarizeMetaAttribution(input.resolverInput, input.journey);
    const metaEvaluationOutcome = metaSummary.metaEvaluationOutcome;
    const decisionReason = input.journey.tier === 'platform_reported_meta'
        ? 'meta_canonical_selected'
        : hasMetaEvidence && hasHigherPrecedenceWinner
            ? 'meta_not_evaluated_higher_precedence_winner'
            : metaEvaluationOutcome === 'eligible_parallel_only'
                ? 'meta_parallel_only_below_confidence_threshold'
                : metaEvaluationOutcome === 'ineligible'
                    ? 'meta_ineligible_failed_hard_guard'
                    : 'meta_not_evaluated_no_evidence';
    const result = await input.client.query(`
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
        $2::uuid,
        $3,
        $4,
        $5,
        $6,
        1,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        NULL,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21,
        $22,
        $23,
        true
      )
      RETURNING id::text
    `, [
        input.order.shopifyOrderId,
        metaSummary.metaAttributionEvidenceId,
        input.backfillRunId ?? null,
        input.resolverRunSource,
        normalizeResolverTriggeredBy(input.resolverTriggeredBy),
        input.journey.resolverRuleVersion,
        input.order.attributionTier ?? 'unattributed',
        input.journey.tier,
        metaEvaluationOutcome,
        metaSummary.metaAffectedCanonical,
        decisionReason,
        hasMetaEvidence ? 'Meta evidence versioning is not fully wired into this resolver path yet.' : null,
        metaSummary.confidenceScore,
        metaSummary.metaPresent ? 0.5 : null,
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
        hasMetaEvidence && !metaSummary.metaAffectedCanonical
    ]);
    return result.rows[0].id;
}
