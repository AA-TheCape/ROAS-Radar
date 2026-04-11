import assert from 'node:assert/strict';
import test from 'node:test';

import { hashIdentityEmail, resolveIdentityStitch } from '../src/modules/identity/index.js';

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
