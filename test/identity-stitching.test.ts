import assert from 'node:assert/strict';
import test from 'node:test';

import { buildIdentityEdgeIngestionMetricsLog, hashIdentityEmail, resolveIdentityStitch } from '../src/modules/identity/index.js';

test('hashIdentityEmail normalizes casing and whitespace before hashing', () => {
  const firstHash = hashIdentityEmail(' Buyer@Example.com ');
  const secondHash = hashIdentityEmail('buyer@example.com');

  assert.equal(firstHash, secondHash);
  assert.match(firstHash ?? '', /^[0-9a-f]{64}$/);
});

test('resolveIdentityStitch reuses an existing identity when identifiers agree', () => {
  const emailHash = hashIdentityEmail('buyer@example.com');
  const decision = resolveIdentityStitch(
    [
      {
        id: 'identity-1',
        hashed_email: emailHash,
        shopify_customer_id: 'shopify-123'
      }
    ],
    {
      shopifyCustomerId: 'shopify-123',
      email: 'buyer@example.com'
    }
  );

  assert.deepEqual(decision, {
    outcome: 'linked',
    identityId: 'identity-1',
    emailHash,
    shopifyCustomerId: 'shopify-123',
    operation: 'reuse'
  });
});

test('resolveIdentityStitch rejects a customer id that conflicts with an existing hashed email', () => {
  const decision = resolveIdentityStitch(
    [
      {
        id: 'identity-1',
        hashed_email: hashIdentityEmail('first@example.com'),
        shopify_customer_id: 'shopify-123'
      }
    ],
    {
      shopifyCustomerId: 'shopify-123',
      email: 'second@example.com'
    }
  );

  assert.equal(decision.outcome, 'conflict');
  assert.equal(decision.reason, 'customer_id_conflicts_with_existing_email');
});

test('resolveIdentityStitch rejects an email hash that conflicts with an existing customer id', () => {
  const decision = resolveIdentityStitch(
    [
      {
        id: 'identity-1',
        hashed_email: hashIdentityEmail('buyer@example.com'),
        shopify_customer_id: 'shopify-123'
      }
    ],
    {
      shopifyCustomerId: 'shopify-456',
      email: 'buyer@example.com'
    }
  );

  assert.equal(decision.outcome, 'conflict');
  assert.equal(decision.reason, 'email_hash_conflicts_with_existing_customer_id');
});

test('resolveIdentityStitch rejects automatic merges across two existing identities', () => {
  const decision = resolveIdentityStitch(
    [
      {
        id: 'identity-1',
        hashed_email: null,
        shopify_customer_id: 'shopify-123'
      },
      {
        id: 'identity-2',
        hashed_email: hashIdentityEmail('buyer@example.com'),
        shopify_customer_id: null
      }
    ],
    {
      shopifyCustomerId: 'shopify-123',
      email: 'buyer@example.com'
    }
  );

  assert.equal(decision.outcome, 'conflict');
  assert.equal(decision.reason, 'identifiers_resolve_to_different_identities');
});

test('buildIdentityEdgeIngestionMetricsLog emits structured counters', () => {
  const payload = JSON.parse(
    buildIdentityEdgeIngestionMetricsLog({
      sourceTable: 'tracking_events',
      evidenceSource: 'tracking_event',
      outcome: 'linked',
      deduplicated: false,
      processedNodes: 3,
      attachedNodes: 2,
      rehomedNodes: 1,
      quarantinedNodes: 0,
      journeyId: '123e4567-e89b-42d3-a456-426614174000'
    })
  ) as Record<string, unknown>;

  assert.equal(payload.event, 'identity_edge_ingestion_processed');
  assert.equal(payload.evidenceSource, 'tracking_event');
  assert.equal(payload.sourceTable, 'tracking_events');
  assert.equal(payload.outcome, 'linked');
  assert.equal(payload.processedNodes, 3);
  assert.equal(payload.attachedNodes, 2);
  assert.equal(payload.rehomedNodes, 1);
  assert.equal(payload.quarantinedNodes, 0);
});
