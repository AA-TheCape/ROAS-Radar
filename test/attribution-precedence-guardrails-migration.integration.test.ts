import assert from 'node:assert/strict';
import test from 'node:test';

import type { PoolClient } from 'pg';

import { resetIntegrationTables, buildRawPayloadFixture } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

async function getPool() {
  const poolModule = await import('../src/db/pool.js');
  return poolModule.pool;
}

async function insertOrder(client: PoolClient, shopifyOrderId: string): Promise<void> {
  const orderFixture = buildRawPayloadFixture({
    id: shopifyOrderId,
    source_name: 'web',
    note_attributes: []
  }, shopifyOrderId);

  await client.query(
    `
      INSERT INTO shopify_orders (
        shopify_order_id,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        source_name,
        payload_external_id,
        payload_size_bytes,
        payload_hash,
        raw_payload,
        ingested_at
      )
      VALUES (
        $1,
        'USD',
        '100.00',
        '100.00',
        '2026-04-30T10:05:00.000Z',
        'web',
        $2,
        $3,
        $4,
        $5::jsonb,
        '2026-04-30T10:06:00.000Z'
      )
    `,
    [
      shopifyOrderId,
      orderFixture.payloadExternalId,
      orderFixture.payloadSizeBytes,
      orderFixture.payloadHash,
      orderFixture.rawPayloadJson
    ]
  );
}

async function insertAttributionResult(
  client: PoolClient,
  input: {
    shopifyOrderId: string;
    attributionReason: string;
  }
): Promise<void> {
  await client.query(
    `
      INSERT INTO attribution_results (
        shopify_order_id,
        attribution_model,
        confidence_score,
        attribution_reason,
        attributed_at,
        reprocess_version,
        model_version,
        match_source,
        confidence_label
      )
      VALUES (
        $1,
        'last_touch',
        1.00,
        $2,
        '2026-04-30T10:07:00.000Z',
        1,
        1,
        $2,
        'high'
      )
    `,
    [input.shopifyOrderId, input.attributionReason]
  );
}

async function insertDecisionArtifact(
  client: PoolClient,
  input: {
    shopifyOrderId: string;
    canonicalTierAfter: string;
    firstPartyWinnerPresent?: boolean;
    shopifyHintWinnerPresent?: boolean;
  }
): Promise<string> {
  const result = await client.query<{ id: string }>(
    `
      INSERT INTO attribution_decision_artifacts (
        shopify_order_id,
        resolver_run_source,
        resolver_triggered_by,
        resolver_timestamp,
        resolver_rule_version,
        resolver_model_version,
        canonical_tier_before,
        canonical_tier_after,
        meta_evaluation_outcome,
        meta_affected_canonical,
        decision_reason,
        rule_inputs_hash,
        evidence_snapshot_hash,
        confidence_threshold,
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
        'forward_processing',
        'integration_test',
        '2026-04-30T10:08:00.000Z',
        'meta-contract-v1',
        1,
        'unattributed',
        $2,
        'not_evaluated',
        false,
        'meta_not_evaluated_higher_precedence_winner',
        'rule-inputs-hash',
        'evidence-snapshot-hash',
        0.50,
        '2026-04-30T10:05:00.000Z',
        'order-snapshot-ref',
        $3,
        $4,
        false,
        $2,
        'integration_test',
        false,
        true
      )
      RETURNING id::text
    `,
    [
      input.shopifyOrderId,
      input.canonicalTierAfter,
      input.firstPartyWinnerPresent ?? false,
      input.shopifyHintWinnerPresent ?? false
    ]
  );

  return result.rows[0].id;
}

test.beforeEach(async () => {
  const pool = await getPool();
  await resetIntegrationTables(pool, [
    'attribution_decision_artifacts',
    'meta_order_attribution_evidence',
    'attribution_results',
    'shopify_orders',
    'tracking_sessions'
  ]);
});

test.after(async () => {
  const pool = await getPool();
  await pool.end();
});

test('migration 0044 rejects platform_reported_meta when attribution_results already imply deterministic_first_party', async () => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await insertOrder(client, 'order-precedence-deterministic-1');
    await insertAttributionResult(client, {
      shopifyOrderId: 'order-precedence-deterministic-1',
      attributionReason: 'matched_by_checkout_token'
    });

    await client.query(
      `
        UPDATE shopify_orders
        SET
          attribution_tier = 'platform_reported_meta',
          attribution_source = 'meta_platform_reported',
          attribution_reason = 'meta_platform_reported_match'
        WHERE shopify_order_id = 'order-precedence-deterministic-1'
      `
    );

    await assert.rejects(
      () => client.query('COMMIT'),
      /Cannot persist canonical tier platform_reported_meta .* higher-precedence deterministic_first_party evidence/
    );
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    client.release();
  }
});

test('migration 0044 rejects ga4_fallback when attribution_results already imply deterministic_shopify_hint', async () => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await insertOrder(client, 'order-precedence-shopify-hint-1');
    await insertAttributionResult(client, {
      shopifyOrderId: 'order-precedence-shopify-hint-1',
      attributionReason: 'shopify_hint_derived'
    });

    await client.query(
      `
        UPDATE shopify_orders
        SET
          attribution_tier = 'ga4_fallback',
          attribution_source = 'ga4_fallback',
          attribution_reason = 'ga4_fallback_match'
        WHERE shopify_order_id = 'order-precedence-shopify-hint-1'
      `
    );

    await assert.rejects(
      () => client.query('COMMIT'),
      /Cannot persist canonical tier ga4_fallback .* higher-precedence deterministic_shopify_hint evidence/
    );
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    client.release();
  }
});

test('migration 0044 rejects decision artifacts that mark lower-precedence winners while first-party evidence is present', async () => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await insertOrder(client, 'order-precedence-artifact-1');

    await assert.rejects(
      () =>
        insertDecisionArtifact(client, {
          shopifyOrderId: 'order-precedence-artifact-1',
          canonicalTierAfter: 'platform_reported_meta',
          firstPartyWinnerPresent: true
        }),
      /Cannot persist canonical tier platform_reported_meta .* deterministic_first_party evidence is present in the decision artifact/
    );
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    client.release();
  }
});

test('migration 0044 rejects linking a lower-precedence shopify_orders tier to a first-party decision artifact', async () => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await insertOrder(client, 'order-precedence-linked-artifact-1');

    const artifactId = await insertDecisionArtifact(client, {
      shopifyOrderId: 'order-precedence-linked-artifact-1',
      canonicalTierAfter: 'deterministic_first_party',
      firstPartyWinnerPresent: true
    });

    await client.query(
      `
        UPDATE shopify_orders
        SET
          attribution_tier = 'platform_reported_meta',
          attribution_source = 'meta_platform_reported',
          attribution_reason = 'meta_platform_reported_match',
          latest_attribution_decision_artifact_id = $2::uuid
        WHERE shopify_order_id = $1
      `,
      ['order-precedence-linked-artifact-1', artifactId]
    );

    await assert.rejects(
      () => client.query('COMMIT'),
      /Cannot persist canonical tier platform_reported_meta .* linked decision artifact resolves to deterministic_first_party/
    );
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    client.release();
  }
});
