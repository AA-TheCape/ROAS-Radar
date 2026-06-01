import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import type { Pool, PoolClient } from 'pg';

import { buildRawPayloadFixture, resetIntegrationTables } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const migrationPath = path.resolve(__dirname, '../db/migrations/0046_add_order_attribution_confidence_metadata.sql');
const rollbackPath = path.resolve(__dirname, '../db/rollbacks/0046_add_order_attribution_confidence_metadata.down.sql');

async function getPool() {
  const poolModule = await import('../src/db/pool.js');
  return poolModule.pool;
}

function stripTransactionWrappers(sql: string): string {
  return sql
    .replace(/^\s*BEGIN;\s*/i, '')
    .replace(/\s*COMMIT;\s*$/i, '');
}

async function resetConfidenceMigrationState(pool: Pool): Promise<void> {
  await resetIntegrationTables(pool, ['attribution_results', 'shopify_orders']);
  await pool.query(stripTransactionWrappers(await readFile(rollbackPath, 'utf8')));
}

async function runConfidenceMigration(client: PoolClient): Promise<void> {
  await client.query(stripTransactionWrappers(await readFile(migrationPath, 'utf8')));
}

async function insertLegacyOrder(client: PoolClient, shopifyOrderId: string, attributionSource: string | null): Promise<void> {
  const orderFixture = buildRawPayloadFixture({
    id: shopifyOrderId,
    source_name: 'web'
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
        attribution_source,
        attribution_reason,
        attribution_matched_at,
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
        'matched_by_checkout_token',
        '2026-04-12T10:07:30.000Z',
        $3,
        $4,
        $5,
        $6::jsonb,
        '2026-04-12T10:06:00.000Z'
      )
    `,
    [
      shopifyOrderId,
      attributionSource,
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
    matchSource: string;
    attributionReason: string;
    confidenceScore: string;
    attributedAt: string;
  }
): Promise<void> {
  await client.query(
    `
      INSERT INTO attribution_results (
        shopify_order_id,
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
        'last_touch',
        'google',
        'cpc',
        'brand-search',
        $2,
        $3,
        $4,
        1,
        1,
        $5,
        'high'
      )
    `,
    [input.shopifyOrderId, input.confidenceScore, input.attributionReason, input.attributedAt, input.matchSource]
  );
}

async function assertRejectsConstraint(pool: Pool, sql: string, pattern: RegExp): Promise<void> {
  await assert.rejects(() => pool.query(sql), pattern);
}

test('migration 0046 backfills confidence lookup metadata and enforces score and lookup constraints', async () => {
  const pool = await getPool();
  await resetConfidenceMigrationState(pool);

  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await insertLegacyOrder(client, 'order-confidence-migration-known', 'checkout_token');
    await insertLegacyOrder(client, 'order-confidence-migration-fallback', 'legacy_unknown_source');

    await insertLegacyAttributionResult(client, {
      shopifyOrderId: 'order-confidence-migration-known',
      matchSource: 'checkout_token',
      attributionReason: 'matched_by_checkout_token',
      confidenceScore: '1.00',
      attributedAt: '2026-04-12T10:07:00.000Z'
    });
    await insertLegacyAttributionResult(client, {
      shopifyOrderId: 'order-confidence-migration-fallback',
      matchSource: 'legacy_unknown_source',
      attributionReason: 'legacy_unknown_method',
      confidenceScore: '0.35',
      attributedAt: '2026-04-12T10:08:00.000Z'
    });

    await runConfidenceMigration(client);
    await client.query('COMMIT');

    const backfilledRows = await pool.query<{
      shopify_order_id: string;
      order_source: string;
      order_method: string;
      order_confidence_score: string;
      order_last_run: Date | null;
      result_source: string;
      result_method: string;
      result_last_run: Date;
    }>(
      `
        SELECT
          orders.shopify_order_id,
          order_sources.code AS order_source,
          order_methods.code AS order_method,
          orders.attribution_confidence_score::text AS order_confidence_score,
          orders.last_attribution_run_at AS order_last_run,
          result_sources.code AS result_source,
          result_methods.code AS result_method,
          results.last_attribution_run_at AS result_last_run
        FROM shopify_orders orders
        JOIN attribution_results results
          ON results.shopify_order_id = orders.shopify_order_id
        JOIN attribution_sources order_sources
          ON order_sources.id = orders.attribution_source_id
        JOIN matching_methods order_methods
          ON order_methods.id = orders.matching_method_id
        JOIN attribution_sources result_sources
          ON result_sources.id = results.attribution_source_id
        JOIN matching_methods result_methods
          ON result_methods.id = results.matching_method_id
        WHERE orders.shopify_order_id LIKE 'order-confidence-migration-%'
        ORDER BY orders.shopify_order_id ASC
      `
    );

    assert.deepEqual(
      backfilledRows.rows.map((row) => ({
        shopifyOrderId: row.shopify_order_id,
        orderSource: row.order_source,
        orderMethod: row.order_method,
        orderConfidenceScore: row.order_confidence_score,
        orderLastRun: row.order_last_run?.toISOString(),
        resultSource: row.result_source,
        resultMethod: row.result_method,
        resultLastRun: row.result_last_run.toISOString()
      })),
      [
        {
          shopifyOrderId: 'order-confidence-migration-fallback',
          orderSource: 'unattributed',
          orderMethod: 'unknown',
          orderConfidenceScore: '0.3500',
          orderLastRun: '2026-04-12T10:07:30.000Z',
          resultSource: 'unattributed',
          resultMethod: 'unknown',
          resultLastRun: '2026-04-12T10:08:00.000Z'
        },
        {
          shopifyOrderId: 'order-confidence-migration-known',
          orderSource: 'checkout_token',
          orderMethod: 'matched_by_checkout_token',
          orderConfidenceScore: '1.0000',
          orderLastRun: '2026-04-12T10:07:30.000Z',
          resultSource: 'checkout_token',
          resultMethod: 'matched_by_checkout_token',
          resultLastRun: '2026-04-12T10:07:00.000Z'
        }
      ]
    );

    await assertRejectsConstraint(
      pool,
      `
        UPDATE shopify_orders
        SET attribution_confidence_score = 1.0001
        WHERE shopify_order_id = 'order-confidence-migration-known'
      `,
      /shopify_orders_attribution_confidence_score_chk/
    );
    await assertRejectsConstraint(
      pool,
      `
        UPDATE attribution_results
        SET confidence_score = -0.01
        WHERE shopify_order_id = 'order-confidence-migration-known'
      `,
      /attribution_results_confidence_score_chk/
    );
    await assertRejectsConstraint(
      pool,
      `
        UPDATE shopify_orders
        SET attribution_source_id = 32767
        WHERE shopify_order_id = 'order-confidence-migration-known'
      `,
      /shopify_orders_attribution_source_id_fkey/
    );
    await assertRejectsConstraint(
      pool,
      `
        UPDATE attribution_results
        SET matching_method_id = 32767
        WHERE shopify_order_id = 'order-confidence-migration-known'
      `,
      /attribution_results_matching_method_id_fkey/
    );
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
    await resetIntegrationTables(pool, ['attribution_results', 'shopify_orders']);
    await pool.query(stripTransactionWrappers(await readFile(migrationPath, 'utf8')));
  }
});
