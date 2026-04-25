import assert from 'node:assert/strict';
import test from 'node:test';

const { __observabilityTestUtils } = await import('../src/observability/index.js');

test('summarizeAttributionObservation classifies complete captures and missing session ids', () => {
  const complete = __observabilityTestUtils.summarizeAttributionObservation({
    roas_radar_session_id: '123e4567-e89b-42d3-a456-426614174000',
    landing_url: 'https://store.example/?utm_source=google',
    page_url: 'https://store.example/products/widget',
    utm_source: 'google',
    gclid: 'GCLID-123'
  });

  assert.equal(complete.captureStatus, 'complete');
});

test('summarizeDualWriteConsistency flags failed server legs as mismatches', () => {
  assert.deepEqual(
    __observabilityTestUtils.summarizeDualWriteConsistency({
      browserOutcome: 'accepted',
      serverOutcome: 'failed'
    }),
    {
      consistencyStatus: 'mismatched',
      browserOutcome: 'accepted',
      serverOutcome: 'failed'
    }
  );
});

test('summarizeResolverOutcome reports unattributed and non-direct winners deterministically', () => {
  const unattributed = __observabilityTestUtils.summarizeResolverOutcome({
    touchpoints: [],
    winner: null
  });

  assert.equal(unattributed.resolverOutcome, 'unattributed');
});
