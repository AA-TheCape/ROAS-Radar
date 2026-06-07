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
const contractPath = path.resolve(__dirname, '../db/operations/0046_contract_order_attribution_confidence_metadata.sql');
const indexesPath = path.resolve(__dirname, '../db/operations/0046_add_order_attribution_confidence_indexes.sql');

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

async function runConfidenceContract(pool: Pool): Promise<void> {
  await pool.query(stripTransactionWrappers(await readFile(contractPath, 'utf8')));
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

test('migration 0046 expands confidence metadata without historical backfill, validation, or large indexes', async () => {
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

    await client.query('COMMIT');
    await runConfidenceMigration(client);

    const expandedRows = await pool.query<{
      shopify_order_id: string;
      order_source_id: number | null;
      order_method_id: number | null;
      order_confidence_score: string | null;
      order_last_run: Date | null;
      result_source_id: number | null;
      result_method_id: number | null;
      result_last_run: Date | null;
    }>(
      `
        SELECT
          orders.shopify_order_id,
          orders.attribution_source_id AS order_source_id,
          orders.matching_method_id AS order_method_id,
          orders.attribution_confidence_score::text AS order_confidence_score,
          orders.last_attribution_run_at AS order_last_run,
          results.attribution_source_id AS result_source_id,
          results.matching_method_id AS result_method_id,
          results.last_attribution_run_at AS result_last_run
        FROM shopify_orders orders
        JOIN attribution_results results
          ON results.shopify_order_id = orders.shopify_order_id
        WHERE orders.shopify_order_id LIKE 'order-confidence-migration-%'
        ORDER BY orders.shopify_order_id ASC
      `
    );

    assert.deepEqual(
      expandedRows.rows.map((row) => ({
        shopifyOrderId: row.shopify_order_id,
        orderSourceId: row.order_source_id,
        orderMethodId: row.order_method_id,
        orderConfidenceScore: row.order_confidence_score,
        orderLastRun: row.order_last_run?.toISOString(),
        resultSourceId: row.result_source_id,
        resultMethodId: row.result_method_id,
        resultLastRun: row.result_last_run?.toISOString()
      })),
      [
        {
          shopifyOrderId: 'order-confidence-migration-fallback',
          orderSourceId: null,
          orderMethodId: null,
          orderConfidenceScore: null,
          orderLastRun: undefined,
          resultSourceId: null,
          resultMethodId: null,
          resultLastRun: undefined
        },
        {
          shopifyOrderId: 'order-confidence-migration-known',
          orderSourceId: null,
          orderMethodId: null,
          orderConfidenceScore: null,
          orderLastRun: undefined,
          resultSourceId: null,
          resultMethodId: null,
          resultLastRun: undefined
        }
      ]
    );

    const validatedConstraints = await pool.query<{ conname: string }>(
      `
        SELECT conname
        FROM pg_constraint
        WHERE conname IN (
          'shopify_orders_attribution_confidence_score_chk',
          'shopify_orders_attribution_confidence_contract_version_chk',
          'shopify_orders_attribution_source_id_fkey',
          'shopify_orders_matching_method_id_fkey',
          'shopify_orders_attribution_source_method_pair_fkey',
          'attribution_results_confidence_score_chk',
          'attribution_results_confidence_contract_version_chk',
          'attribution_results_attribution_source_id_fkey',
          'attribution_results_matching_method_id_fkey',
          'attribution_results_attribution_source_method_pair_fkey'
        )
          AND convalidated = true
      `
    );
    assert.equal(validatedConstraints.rowCount, 0);

    const indexes = await pool.query<{ indexname: string }>(
      `
        SELECT indexname
        FROM pg_indexes
        WHERE indexname IN (
          'shopify_orders_attribution_source_run_idx',
          'shopify_orders_matching_method_run_idx',
          'shopify_orders_attribution_confidence_idx',
          'attribution_results_source_attributed_idx',
          'attribution_results_method_attributed_idx',
          'attribution_results_last_run_idx'
        )
      `
    );
    assert.equal(indexes.rowCount, 0);

    const indexSql = await readFile(indexesPath, 'utf8');
    assert.match(indexSql, /CREATE INDEX CONCURRENTLY IF NOT EXISTS shopify_orders_attribution_source_run_idx/);
    assert.match(indexSql, /CREATE INDEX CONCURRENTLY IF NOT EXISTS attribution_results_last_run_idx/);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
    await resetIntegrationTables(pool, ['attribution_results', 'shopify_orders']);
    await pool.query(stripTransactionWrappers(await readFile(migrationPath, 'utf8')));
  }
});

test('confidence metadata contract is applied only after resumable backfill completes', async () => {
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

    await client.query('COMMIT');
    await runConfidenceMigration(client);

    await assert.rejects(
      () => runConfidenceContract(pool),
      /confidence metadata contract blocked: incomplete_orders=2, incomplete_results=2, incomplete_credits=0/
    );

    const { backfillOrderAttributionConfidenceMetadata } = await import(
      '../src/modules/attribution/confidence-backfill.js'
    );
    const report = await backfillOrderAttributionConfidenceMetadata({
      workerId: 'confidence-migration-contract-test',
      batchSize: 1
    });
    assert.equal(report.scannedOrders >= 2, true);
    assert.equal(report.updatedOrders >= 2, true);
    assert.equal(report.updatedResults >= 2, true);

    await runConfidenceContract(pool);

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
          orderSource: 'checkout_token',
          orderMethod: 'matched_by_checkout_token',
          orderConfidenceScore: '0.35',
          orderLastRun: '2026-04-12T10:07:30.000Z',
          resultSource: 'checkout_token',
          resultMethod: 'matched_by_checkout_token',
          resultLastRun: '2026-04-12T10:07:30.000Z'
        },
        {
          shopifyOrderId: 'order-confidence-migration-known',
          orderSource: 'checkout_token',
          orderMethod: 'matched_by_checkout_token',
          orderConfidenceScore: '1.00',
          orderLastRun: '2026-04-12T10:07:30.000Z',
          resultSource: 'checkout_token',
          resultMethod: 'matched_by_checkout_token',
          resultLastRun: '2026-04-12T10:07:30.000Z'
        }
      ]
    );

    await assertRejectsConstraint(
      pool,
      `
        UPDATE shopify_orders
        SET attribution_confidence_score = 1.01
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
    await assertRejectsConstraint(
      pool,
      `
        UPDATE shopify_orders
        SET
          attribution_source_id = 2,
          matching_method_id = 1
        WHERE shopify_order_id = 'order-confidence-migration-known'
      `,
      /shopify_orders_attribution_source_method_pair_fkey/
    );
    await assertRejectsConstraint(
      pool,
      `
        UPDATE attribution_results
        SET
          attribution_source_id = 2,
          matching_method_id = 1
        WHERE shopify_order_id = 'order-confidence-migration-known'
      `,
      /attribution_results_attribution_source_method_pair_fkey/
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
