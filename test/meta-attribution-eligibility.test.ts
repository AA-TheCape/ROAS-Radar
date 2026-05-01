import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const { evaluateMetaAttributionEligibility, resolveMetaAttributionThresholds } = await import(
  '../src/modules/meta-ads/meta-attribution-eligibility.js'
);

function buildInput(
  overrides: Partial<Parameters<typeof evaluateMetaAttributionEligibility>[0]> = {}
): Parameters<typeof evaluateMetaAttributionEligibility>[0] {
  return {
    orderOccurredAtUtc: new Date('2026-04-10T18:00:00.000Z'),
    metaTouchpointOccurredAtUtc: new Date('2026-04-09T18:00:00.000Z'),
    attributionWindowDays: 7,
    sourceKind: 'order_joinable',
    matchBasis: 'fbc',
    confidenceScore: 0.5,
    rawPayloadReference: 'meta_raw:123',
    rawRecordId: 22,
    ingestionRunId: 9,
    normalizationFailures: [],
    ...overrides
  };
}

test('meta eligibility marks the canonical threshold boundary as eligible_canonical', () => {
  const result = evaluateMetaAttributionEligibility(buildInput({ confidenceScore: 0.5 }));

  assert.equal(result.eligibilityOutcome, 'eligible_canonical');
  assert.equal(result.reasonCode, 'meta_canonical_selected');
  assert.deepEqual(result.eligibilityReasons, ['meta_canonical_selected']);
  assert.deepEqual(result.disqualificationReasons, []);
  assert.equal(result.eligibilitySignals.confidenceAtLeastCanonical, true);
  assert.equal(result.eligibilitySignals.confidenceWithinParallelBand, false);
});

test('meta eligibility marks the parallel threshold boundary as eligible_parallel_only', () => {
  const result = evaluateMetaAttributionEligibility(buildInput({ confidenceScore: 0.35 }));

  assert.equal(result.eligibilityOutcome, 'eligible_parallel_only');
  assert.equal(result.reasonCode, 'meta_parallel_only_below_confidence_threshold');
  assert.deepEqual(result.eligibilityReasons, ['meta_parallel_only_below_confidence_threshold']);
  assert.deepEqual(result.parallelOnlyReasons, ['confidence_below_canonical_threshold']);
  assert.deepEqual(result.disqualificationReasons, []);
  assert.equal(result.eligibilitySignals.confidenceAtLeastCanonical, false);
  assert.equal(result.eligibilitySignals.confidenceWithinParallelBand, true);
  assert.equal(result.eligibilitySignals.confidenceBelowParallelFloor, false);
});

test('meta eligibility marks values below the parallel floor as ineligible', () => {
  const result = evaluateMetaAttributionEligibility(buildInput({ confidenceScore: 0.3499 }));

  assert.equal(result.eligibilityOutcome, 'ineligible');
  assert.equal(result.reasonCode, 'meta_ineligible_below_parallel_threshold');
  assert.deepEqual(result.eligibilityReasons, ['meta_ineligible_below_parallel_threshold']);
  assert.deepEqual(result.disqualificationReasons, ['confidence_below_parallel_floor']);
  assert.equal(result.eligibilitySignals.confidenceBelowParallelFloor, true);
});

test('meta eligibility keeps missing required fields ahead of scoring outcomes', () => {
  const result = evaluateMetaAttributionEligibility(
    buildInput({
      confidenceScore: 0.9,
      ingestionRunId: null,
      rawPayloadReference: null,
      rawRecordId: null
    })
  );

  assert.equal(result.eligibilityOutcome, 'ineligible');
  assert.equal(result.reasonCode, 'meta_ineligible_missing_required_fields');
  assert.deepEqual(result.eligibilityReasons, ['meta_ineligible_missing_required_fields']);
  assert.deepEqual(result.disqualificationReasons, [
    'missing_raw_payload_traceability',
    'missing_ingestion_run_reference'
  ]);
});

test('meta eligibility keeps hard guards ahead of scoring outcomes', () => {
  const result = evaluateMetaAttributionEligibility(
    buildInput({
      confidenceScore: 0.9,
      metaTouchpointOccurredAtUtc: new Date('2026-04-10T18:01:00.000Z')
    })
  );

  assert.equal(result.eligibilityOutcome, 'ineligible');
  assert.equal(result.reasonCode, 'meta_ineligible_failed_hard_guard');
  assert.deepEqual(result.eligibilityReasons, ['meta_ineligible_failed_hard_guard']);
  assert.deepEqual(result.disqualificationReasons, ['meta_touchpoint_after_order', 'outside_attribution_window']);
});

test('meta eligibility supports custom threshold overrides at boundary values', () => {
  const result = evaluateMetaAttributionEligibility(buildInput({ confidenceScore: 0.6 }), {
    canonicalThreshold: 0.6,
    parallelThreshold: 0.4
  });

  assert.equal(result.eligibilityOutcome, 'eligible_canonical');
  assert.deepEqual(result.thresholds, {
    canonicalThreshold: 0.6,
    parallelThreshold: 0.4
  });

  const parallelResult = evaluateMetaAttributionEligibility(buildInput({ confidenceScore: 0.4 }), {
    canonicalThreshold: 0.6,
    parallelThreshold: 0.4
  });

  assert.equal(parallelResult.eligibilityOutcome, 'eligible_parallel_only');
  assert.deepEqual(parallelResult.thresholds, {
    canonicalThreshold: 0.6,
    parallelThreshold: 0.4
  });
});

test('meta eligibility threshold configuration rejects invalid ranges', () => {
  assert.throws(
    () =>
      resolveMetaAttributionThresholds({
        canonicalThreshold: 0.3,
        parallelThreshold: 0.4
      }),
    /Invalid Meta attribution thresholds/
  );
});
