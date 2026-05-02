import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import type { PoolClient } from 'pg';

import { buildRawPayloadFixture, resetIntegrationTables } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const migrationPath = path.resolve(__dirname, '../db/migrations/0037_add_shopify_order_attribution_tiers.sql');

async function getPool() {
  const poolModule = await import('../src/db/pool.js');
  return poolModule.pool;
}

function stripTransactionWrappers(sql: string): string {
  return sql
    .replace(/^\s*BEGIN;\s*/i, '')
    .replace(/\s*COMMIT;\s*$/i, '');
}

async function insertLegacyOrder(client: PoolClient, shopifyOrderId: string): Promise<void> {
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
        '2026-04-12T10:05:00.000Z',
        'web',
        $2,
        $3,
        $4,
        $5::jsonb,
        '2026-04-12T10:06:00.000Z'
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

async function insertLegacyAttributionResult(
  client: PoolClient,
  input: {
    shopifyOrderId: string;
    sessionId?: string | null;
    attributionReason: string;
    attributedSource?: string | null;
    attributedMedium?: string | null;
    attributedCampaign?: string | null;
  }
): Promise<void> {
  await client.query(
    `
      INSERT INTO attribution_results (
        shopify_order_id,
        session_id,
        attribution_model,
        attributed_source,
        attributed_medium,
        attributed_campaign,
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
        $2::uuid,
        'last_touch',
        $3,
        $4,
        $5,
        0.35,
        $6,
        '2026-04-12T10:07:00.000Z',
        1,
        1,
        $7,
        'medium'
      )
    `,
    [
      input.shopifyOrderId,
      input.sessionId ?? null,
      input.attributedSource ?? null,
      input.attributedMedium ?? null,
      input.attributedCampaign ?? null,
      input.attributionReason,
      input.attributionReason
    ]
  );
}

test('migration 0037 preserves GA4 legacy rows as ga4_fallback without changing deterministic or unattributed backfills', async () => {
  const pool = await getPool();
  await resetIntegrationTables(pool, ['attribution_results', 'shopify_orders', 'tracking_sessions']);

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query(`
      ALTER TABLE shopify_orders
        DROP CONSTRAINT IF EXISTS shopify_orders_attribution_tier_chk
    `);
    await client.query(`
      ALTER TABLE shopify_orders
        DROP COLUMN IF EXISTS attribution_tier,
        DROP COLUMN IF EXISTS attribution_source,
        DROP COLUMN IF EXISTS attribution_matched_at,
        DROP COLUMN IF EXISTS attribution_reason
    `);

    const sessionResult = await client.query<{ id: string }>(
      'INSERT INTO tracking_sessions DEFAULT VALUES RETURNING id::text'
    );
    const deterministicSessionId = sessionResult.rows[0].id;

    await insertLegacyOrder(client, 'order-tier-migration-ga4-current');
    await insertLegacyOrder(client, 'order-tier-migration-ga4-legacy');
    await insertLegacyOrder(client, 'order-tier-migration-deterministic');
    await insertLegacyOrder(client, 'order-tier-migration-unattributed');

    await insertLegacyAttributionResult(client, {
      shopifyOrderId: 'order-tier-migration-ga4-current',
      attributionReason: 'ga4_fallback_match',
      attributedSource: 'google',
      attributedMedium: 'cpc',
      attributedCampaign: 'brand-search'
    });
    await insertLegacyAttributionResult(client, {
      shopifyOrderId: 'order-tier-migration-ga4-legacy',
      attributionReason: 'legacy_seed',
      attributedSource: 'google',
      attributedMedium: 'paid_social',
      attributedCampaign: 'retargeting'
    });
    await insertLegacyAttributionResult(client, {
      shopifyOrderId: 'order-tier-migration-deterministic',
      sessionId: deterministicSessionId,
      attributionReason: 'matched_by_checkout_token',
      attributedSource: 'google',
      attributedMedium: 'cpc',
      attributedCampaign: 'spring-sale'
    });
    await insertLegacyAttributionResult(client, {
      shopifyOrderId: 'order-tier-migration-unattributed',
      attributionReason: 'unattributed'
    });

    const migrationSql = stripTransactionWrappers(await readFile(migrationPath, 'utf8'));
    await client.query(migrationSql);

    const backfilledRows = await client.query<{
      shopify_order_id: string;
      attribution_tier: string | null;
      attribution_source: string | null;
      attribution_reason: string | null;
    }>(
      `
        SELECT
          shopify_order_id,
          attribution_tier,
          attribution_source,
          attribution_reason
        FROM shopify_orders
        WHERE shopify_order_id LIKE 'order-tier-migration-%'
        ORDER BY shopify_order_id ASC
      `
    );

    assert.deepEqual(backfilledRows.rows, [
      {
        shopify_order_id: 'order-tier-migration-deterministic',
        attribution_tier: 'deterministic_first_party',
        attribution_source: 'checkout_token',
        attribution_reason: 'matched_by_checkout_token'
      },
      {
        shopify_order_id: 'order-tier-migration-ga4-current',
        attribution_tier: 'ga4_fallback',
        attribution_source: 'ga4_fallback',
        attribution_reason: 'ga4_fallback_match'
      },
      {
        shopify_order_id: 'order-tier-migration-ga4-legacy',
        attribution_tier: 'ga4_fallback',
        attribution_source: 'ga4_fallback',
        attribution_reason: 'legacy_seed'
      },
      {
        shopify_order_id: 'order-tier-migration-unattributed',
        attribution_tier: 'unattributed',
        attribution_source: 'unattributed',
        attribution_reason: 'unattributed'
      }
    ]);
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    client.release();
  }
});
