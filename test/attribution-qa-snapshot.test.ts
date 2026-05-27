import assert from 'node:assert/strict';
import test from 'node:test';

import type { AttributionCandidateExtractionResult } from '../src/modules/attribution/candidate-extraction.js';
import { ATTRIBUTION_MODELS, executeAttributionModels } from '../src/modules/attribution/engine.js';
import { buildAttributionQaSnapshot } from '../src/modules/attribution/qa-snapshot.js';
import { resolveAttributionTier } from '../src/modules/attribution/resolver.js';

const order = {
  shopify_order_id: 'qa-order-1',
  name: '#1001',
  currency_code: 'USD',
  subtotal_price: '90.00',
  total_price: '100.00',
  processed_at: new Date('2026-04-10T12:00:00.000Z'),
  created_at_shopify: null,
  ingested_at: new Date('2026-04-10T12:01:00.000Z'),
  landing_session_id: '123e4567-e89b-42d3-a456-426614174000',
  checkout_token: null,
  cart_token: null,
  shopify_customer_id: null,
  email_hash: null,
  customer_identity_id: null,
  identity_journey_id: null,
  source_name: 'web',
  raw_payload: { name: '#1001' }
};

function deterministicCandidate(overrides: Partial<AttributionCandidateExtractionResult['deterministicFirstParty'][number]> = {}) {
  return {
    sourceClass: 'deterministic_first_party' as const,
    sourceKey: order.landing_session_id,
    sessionId: order.landing_session_id,
    sourceTouchEventId: 'event-landing-1',
    ingestionSource: 'landing_session_id' as const,
    occurredAtUtc: new Date('2026-04-10T10:00:00.000Z'),
    source: 'google',
    medium: 'cpc',
    campaign: 'brand',
    content: null,
    term: null,
    clickIdType: 'gclid',
    clickIdValue: 'GCLID-1',
    attributionReason: 'matched_by_landing_session',
    confidenceScore: 1,
    isDirect: false,
    isSynthetic: false,
    ...overrides
  };
}

function ga4Candidate(overrides: Partial<AttributionCandidateExtractionResult['ga4Fallback'][number]> = {}) {
  return {
    sourceClass: 'ga4_fallback' as const,
    sourceKey: 'ga4:transaction:qa-order-1',
    sessionId: null,
    sourceTouchEventId: null,
    ingestionSource: 'ga4_fallback' as const,
    occurredAtUtc: new Date('2026-04-10T11:00:00.000Z'),
    source: 'google',
    medium: 'cpc',
    campaign: 'ga4-brand',
    content: null,
    term: null,
    clickIdType: 'gclid',
    clickIdValue: 'GCLID-GA4',
    attributionReason: 'ga4_fallback_match',
    confidenceScore: 0.35,
    isDirect: false,
    isSynthetic: true,
    ...overrides
  };
}

function executeFor(candidates: AttributionCandidateExtractionResult) {
  const journey = resolveAttributionTier(candidates);
  const execution = executeAttributionModels(journey.touchpoints, {
    orderOccurredAt: journey.orderOccurredAtUtc ?? order.ingested_at,
    orderRevenue: order.total_price,
    attributionModels: ATTRIBUTION_MODELS,
    normalizationFailuresCount: journey.normalizationFailures.length
  });

  return { journey, execution };
}

test('attribution QA snapshot preserves all candidates and explains why GA4 lost to deterministic attribution', () => {
  const candidates: AttributionCandidateExtractionResult = {
    orderOccurredAtUtc: order.processed_at,
    orderTimestampSource: 'processed_at',
    deterministicFirstParty: [deterministicCandidate()],
    shopifyHint: [],
    ga4Fallback: [ga4Candidate()],
    normalizationFailures: []
  };
  const { journey, execution } = executeFor(candidates);

  const snapshot = buildAttributionQaSnapshot({
    order,
    candidates,
    journey,
    execution,
    generatedAt: new Date('2026-04-10T12:05:00.000Z')
  });

  assert.equal(snapshot.outcome.status, 'success');
  assert.equal(snapshot.outcome.attribution_tier, 'deterministic_first_party');
  assert.equal(snapshot.candidates.deterministic_first_party[0].selected, true);
  assert.equal(snapshot.candidates.ga4_fallback[0].selected, false);
  assert.match(
    snapshot.explainability.find((record) => record.explain_stage === 'fallback')?.decision_reason ?? '',
    /ga4_fallback_not_used_deterministic_first_party/
  );
  assert.ok(
    snapshot.explainability.some(
      (record) =>
        record.touchpoint_id === 'ga4:transaction:qa-order-1' &&
        record.decision_reason === 'blocked_by_deterministic_first_party_winner'
    )
  );
});

test('attribution QA snapshot records winner reason and selected model for GA4 fallback winners', () => {
  const candidates: AttributionCandidateExtractionResult = {
    orderOccurredAtUtc: order.processed_at,
    orderTimestampSource: 'processed_at',
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [ga4Candidate()],
    normalizationFailures: []
  };
  const { journey, execution } = executeFor(candidates);

  const snapshot = buildAttributionQaSnapshot({
    order,
    candidates,
    journey,
    execution,
    generatedAt: new Date('2026-04-10T12:05:00.000Z')
  });

  assert.equal(snapshot.outcome.status, 'success');
  assert.equal(snapshot.outcome.attribution_tier, 'ga4_fallback');
  assert.equal(snapshot.outcome.attribution_reason, 'ga4_fallback_match');
  assert.equal(snapshot.outcome.selected_model_key, 'hinted_fallback_only');
  assert.equal(snapshot.candidates.ga4_fallback[0].selected, true);
  assert.ok(
    snapshot.explainability.some(
      (record) =>
        record.decision === 'winner' &&
        record.decision_reason === 'selected_ga4_fallback' &&
        record.details_json.winner_reason === 'ga4_fallback_match'
    )
  );
  assert.ok(
    snapshot.explainability.some(
      (record) =>
        record.explain_stage === 'fallback' &&
        record.decision === 'fallback_used' &&
        record.decision_reason === 'ga4_fallback_selected_after_no_higher_tier_winner'
    )
  );
});

test('attribution QA snapshot records no-match diagnostics including missing order fields', () => {
  const candidates: AttributionCandidateExtractionResult = {
    orderOccurredAtUtc: order.processed_at,
    orderTimestampSource: 'processed_at',
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [],
    normalizationFailures: []
  };
  const noIdentifierOrder = {
    ...order,
    landing_session_id: null
  };
  const { journey, execution } = executeFor(candidates);

  const snapshot = buildAttributionQaSnapshot({
    order: noIdentifierOrder,
    candidates,
    journey,
    execution,
    generatedAt: new Date('2026-04-10T12:05:00.000Z')
  });

  assert.equal(snapshot.outcome.status, 'no_match');
  assert.equal(snapshot.outcome.match_source, 'unattributed');
  assert.ok(snapshot.diagnostics.normalization_failures.some((failure) => failure.reason === 'missing_landing_session_id'));
  assert.ok(snapshot.diagnostics.notes.some((note) => note === 'ga4 fallback evaluated no candidates'));
  assert.ok(
    snapshot.explainability.some(
      (record) => record.explain_stage === 'fallback' && record.decision_reason === 'ga4_fallback_no_eligible_candidate'
    )
  );
});

test('attribution QA snapshot carries extractor failures and future-candidate exclusion reasons', () => {
  const candidates: AttributionCandidateExtractionResult = {
    orderOccurredAtUtc: order.processed_at,
    orderTimestampSource: 'processed_at',
    deterministicFirstParty: [
      deterministicCandidate({
        sourceKey: 'landing_session_id:future',
        sourceTouchEventId: 'event-after-order',
        occurredAtUtc: new Date('2026-04-10T12:30:00.000Z')
      })
    ],
    shopifyHint: [],
    ga4Fallback: [],
    normalizationFailures: [
      {
        scope: 'ga4_fallback',
        reason: 'missing_session_identity',
        sourceKey: 'ga4:transaction:qa-order-1'
      }
    ]
  };
  const { journey, execution } = executeFor(candidates);

  const snapshot = buildAttributionQaSnapshot({
    order,
    candidates,
    journey,
    execution,
    generatedAt: new Date('2026-04-10T12:05:00.000Z')
  });

  assert.equal(snapshot.outcome.status, 'no_match');
  assert.ok(
    snapshot.diagnostics.normalization_failures.some(
      (failure) =>
        failure.scope === 'ga4_fallback' &&
        failure.reason === 'missing_session_identity' &&
        failure.source_key === 'ga4:transaction:qa-order-1'
    )
  );
  assert.ok(
    snapshot.explainability.some(
      (record) => record.touchpoint_id === 'event-after-order' && record.decision_reason === 'no_winner_selected'
    )
  );
});
