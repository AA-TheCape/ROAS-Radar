import assert from 'node:assert/strict';
import test from 'node:test';

import {
  boundConfidenceScore,
  buildAttributionConfidenceMetadata,
  confidenceScoreForWinner
} from '../src/modules/attribution/confidence-scoring.js';

test('boundConfidenceScore returns a bounded four-decimal confidence score', () => {
  assert.equal(boundConfidenceScore(1.25), 1);
  assert.equal(boundConfidenceScore(-0.2), 0);
  assert.equal(boundConfidenceScore(Number.NaN, 0.35), 0.35);
  assert.equal(boundConfidenceScore(0.123456), 0.1235);
});

test('confidenceScoreForWinner applies default scores by matching source', () => {
  assert.equal(confidenceScoreForWinner(null), 0);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'landing_session_id' }), 1);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'checkout_token' }), 1);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'cart_token' }), 0.9);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'customer_identity' }), 0.6);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'shopify_marketing_hint' }), 0.55);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'ga4_fallback' }), 0.35);
});

test('confidenceScoreForWinner bounds explicit candidate scores', () => {
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'ga4_fallback' }, 1.75), 1);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'shopify_marketing_hint' }, -0.5), 0);
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'ga4_fallback' }, 0.333333), 0.3333);
});

test('buildAttributionConfidenceMetadata returns persistable confidence metadata', () => {
  const lastAttributionRunAt = new Date('2026-01-02T03:04:05.000Z');
  const metadata = buildAttributionConfidenceMetadata({
    journey: {
      confidenceScore: 1.2,
      attributionReason: 'matched_by_checkout_token'
    },
    attributionSourceCode: 'checkout_token',
    lastAttributionRunAt
  });

  assert.deepEqual(metadata, {
    confidenceScore: 1,
    attributionSourceCode: 'checkout_token',
    matchingMethodCode: 'matched_by_checkout_token',
    lastAttributionRunAt
  });
});
