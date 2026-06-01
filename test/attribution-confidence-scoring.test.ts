import assert from 'node:assert/strict';
import test from 'node:test';

import {
  attributionConfidenceFingerprintChanged,
  boundConfidenceScore,
  buildAttributionConfidenceMetadata,
  confidenceScoreForWinner
} from '../src/modules/attribution/confidence-scoring.js';
import { buildAttributionConfidenceLabel } from '../src/modules/attribution/order-attribution-audit.js';

test('boundConfidenceScore returns a bounded two-decimal confidence score', () => {
  assert.equal(boundConfidenceScore(1.25), 1);
  assert.equal(boundConfidenceScore(-0.2), 0);
  assert.equal(boundConfidenceScore(Number.NaN, 0.35), 0.35);
  assert.equal(boundConfidenceScore(0.123456), 0.12);
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
  assert.equal(confidenceScoreForWinner({ ingestionSource: 'ga4_fallback' }, 0.333333), 0.33);
});

test('buildAttributionConfidenceLabel follows the v1 score table', () => {
  assert.equal(buildAttributionConfidenceLabel(1), 'high');
  assert.equal(buildAttributionConfidenceLabel(0.9), 'high');
  assert.equal(buildAttributionConfidenceLabel(0.6), 'medium');
  assert.equal(buildAttributionConfidenceLabel(0.55), 'low');
  assert.equal(buildAttributionConfidenceLabel(0.4), 'low');
  assert.equal(buildAttributionConfidenceLabel(0.35), 'low');
  assert.equal(buildAttributionConfidenceLabel(0.25), 'low');
  assert.equal(buildAttributionConfidenceLabel(0), 'none');
  assert.throws(() => buildAttributionConfidenceLabel(0.5), /Unsupported attribution confidence score/);
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
    confidenceContractVersion: 'v1',
    lastAttributionRunAt
  });
});

test('buildAttributionConfidenceMetadata normalizes identity journey matching method', () => {
  const lastAttributionRunAt = new Date('2026-01-02T03:04:05.000Z');
  const metadata = buildAttributionConfidenceMetadata({
    journey: {
      confidenceScore: 0.6,
      attributionReason: 'matched_by_identity_journey'
    },
    attributionSourceCode: 'customer_identity',
    lastAttributionRunAt
  });

  assert.equal(metadata.matchingMethodCode, 'matched_by_customer_identity');
});

const attributionFingerprint = {
  sessionId: '11111111-1111-4111-8111-111111111111',
  attributedSource: 'google',
  attributedMedium: 'cpc',
  attributedCampaign: 'brand',
  attributedContent: null,
  attributedTerm: null,
  attributedClickIdType: 'gclid',
  attributedClickIdValue: 'gclid-1',
  confidenceScore: 1,
  attributionReason: 'matched_by_landing_session',
  modelVersion: 1,
  matchSource: 'deterministic_first_party',
  attributionSourceCode: 'landing_session_id',
  matchingMethodCode: 'matched_by_landing_session',
  confidenceContractVersion: 'v1'
};

test('attributionConfidenceFingerprintChanged ignores unchanged attribution fields', () => {
  assert.equal(attributionConfidenceFingerprintChanged(attributionFingerprint, { ...attributionFingerprint }), false);
});

test('attributionConfidenceFingerprintChanged detects source and method changes', () => {
  assert.equal(
    attributionConfidenceFingerprintChanged(attributionFingerprint, {
      ...attributionFingerprint,
      attributionSourceCode: 'checkout_token'
    }),
    true
  );
  assert.equal(
    attributionConfidenceFingerprintChanged(attributionFingerprint, {
      ...attributionFingerprint,
      matchingMethodCode: 'matched_by_checkout_token'
    }),
    true
  );
});
