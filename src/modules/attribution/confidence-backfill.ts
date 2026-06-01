import type { PoolClient } from 'pg';

import { withTransaction } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';

const DEFAULT_BATCH_SIZE = 500;
const DEFAULT_MAX_RETRIES = 3;
const MAX_REPORTED_FAILURES = 100;

type ConfidenceBackfillCursor = {
  lastOrderRowId: string | null;
  completed: boolean;
  batchesProcessed: number;
};

export type OrderAttributionConfidenceBackfillProgress = {
  scannedOrders: number;
  updatedOrders: number;
  updatedResults: number;
  fallbackRows: number;
  failedBatches: number;
  failures: Array<{
    afterOrderRowId: string | null;
    code: string;
    message: string;
  }>;
  cursor: ConfidenceBackfillCursor;
};

export type OrderAttributionConfidenceBackfillReport = OrderAttributionConfidenceBackfillProgress & {
  workerId: string;
  dryRun: boolean;
  batchSize: number;
  maxRetries: number;
};

type ConfidenceBackfillBatchResult = {
  scannedOrders: number;
  updatedOrders: number;
  updatedResults: number;
  fallbackRows: number;
  lastOrderRowId: string | null;
};

type ExecuteConfidenceBackfillBatchInput = {
  afterOrderRowId: string | null;
  batchSize: number;
  dryRun: boolean;
};

type ExecuteConfidenceBackfillBatch = (
  input: ExecuteConfidenceBackfillBatchInput
) => Promise<ConfidenceBackfillBatchResult>;

export type BackfillOrderAttributionConfidenceOptions = {
  workerId: string;
  batchSize?: number;
  maxRetries?: number;
  dryRun?: boolean;
  progress?: OrderAttributionConfidenceBackfillProgress;
  onProgress?: (progress: OrderAttributionConfidenceBackfillProgress) => Promise<void> | void;
  executeBatch?: ExecuteConfidenceBackfillBatch;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeNonNegativeNumber(value: unknown): number {
  const normalized = Number(value ?? 0);
  return Number.isFinite(normalized) && normalized >= 0 ? normalized : 0;
}

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value)) {
    return fallback;
  }

  return Math.max(1, Math.trunc(value ?? fallback));
}

function normalizeFailureCode(error: unknown): string {
  if (isRecord(error) && typeof error.code === 'string' && error.code.trim()) {
    return error.code.trim();
  }

  if (error instanceof Error && error.name.trim()) {
    return error.name.trim();
  }

  return 'order_attribution_confidence_backfill_failed';
}

function normalizeFailureMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }

  if (typeof error === 'string' && error.trim()) {
    return error.trim();
  }

  return 'Order attribution confidence backfill failed';
}

function recordFailure(
  failures: OrderAttributionConfidenceBackfillProgress['failures'],
  failure: OrderAttributionConfidenceBackfillProgress['failures'][number]
): void {
  if (failures.length < MAX_REPORTED_FAILURES) {
    failures.push(failure);
  }
}

export function buildEmptyOrderAttributionConfidenceBackfillProgress(): OrderAttributionConfidenceBackfillProgress {
  return {
    scannedOrders: 0,
    updatedOrders: 0,
    updatedResults: 0,
    fallbackRows: 0,
    failedBatches: 0,
    failures: [],
    cursor: {
      lastOrderRowId: null,
      completed: false,
      batchesProcessed: 0
    }
  };
}

export function parseOrderAttributionConfidenceBackfillProgress(value: unknown): OrderAttributionConfidenceBackfillProgress {
  const defaults = buildEmptyOrderAttributionConfidenceBackfillProgress();
  const record = isRecord(value) ? value : {};
  const cursor = isRecord(record.cursor) ? record.cursor : {};

  return {
    scannedOrders: normalizeNonNegativeNumber(record.scannedOrders),
    updatedOrders: normalizeNonNegativeNumber(record.updatedOrders),
    updatedResults: normalizeNonNegativeNumber(record.updatedResults),
    fallbackRows: normalizeNonNegativeNumber(record.fallbackRows),
    failedBatches: normalizeNonNegativeNumber(record.failedBatches),
    failures: Array.isArray(record.failures)
      ? (record.failures as OrderAttributionConfidenceBackfillProgress['failures'])
      : defaults.failures,
    cursor: {
      lastOrderRowId: typeof cursor.lastOrderRowId === 'string' ? cursor.lastOrderRowId : null,
      completed: cursor.completed === true,
      batchesProcessed: normalizeNonNegativeNumber(cursor.batchesProcessed)
    }
  };
}

export async function executeOrderAttributionConfidenceBackfillBatch(
  client: PoolClient,
  input: ExecuteConfidenceBackfillBatchInput
): Promise<ConfidenceBackfillBatchResult> {
  const result = await client.query<{
    scanned_orders: string;
    updated_orders: string;
    updated_results: string;
    fallback_rows: string;
    last_order_row_id: string | null;
  }>(
    `
      WITH candidates AS (
        SELECT
          orders.id,
          orders.shopify_order_id
        FROM shopify_orders orders
        LEFT JOIN attribution_results results
          ON results.shopify_order_id = orders.shopify_order_id
        WHERE orders.id > $1::bigint
          AND (
            results.id IS NOT NULL
            OR orders.attribution_tier IS NOT NULL
            OR orders.attribution_source IS NOT NULL
            OR orders.attribution_reason IS NOT NULL
            OR orders.attribution_snapshot IS NOT NULL
          )
        ORDER BY orders.id ASC
        LIMIT $2
      ),
      raw_metadata AS (
        SELECT
          candidates.id AS order_row_id,
          orders.shopify_order_id,
          results.id AS result_id,
          COALESCE(orders.attribution_reason, results.attribution_reason) AS raw_method_code,
          COALESCE(orders.attribution_source, results.match_source) AS raw_source_code,
          COALESCE(
            orders.last_attribution_run_at,
            results.last_attribution_run_at,
            orders.attribution_matched_at,
            results.attributed_at,
            orders.attribution_snapshot_updated_at,
            orders.processed_at,
            orders.created_at_shopify,
            orders.ingested_at,
            now()
          ) AS last_attribution_run_at,
          LEAST(
            GREATEST(
              COALESCE(
                results.confidence_score::numeric,
                orders.attribution_confidence_score::numeric,
                0
              ),
              0
            ),
            1
          )::numeric(5, 4) AS confidence_score
        FROM candidates
        JOIN shopify_orders orders
          ON orders.id = candidates.id
        LEFT JOIN attribution_results results
          ON results.shopify_order_id = candidates.shopify_order_id
      ),
      normalized_metadata AS (
        SELECT
          *,
          CASE
            WHEN raw_method_code = 'matched_by_landing_session' THEN 'landing_session_id'
            WHEN raw_method_code = 'matched_by_checkout_token' THEN 'checkout_token'
            WHEN raw_method_code = 'matched_by_cart_token' THEN 'cart_token'
            WHEN raw_method_code = 'matched_by_customer_identity' THEN 'customer_identity'
            WHEN raw_method_code = 'matched_by_identity_journey' THEN 'stitched_identity_journey'
            WHEN raw_method_code IN ('shopify_hint_derived', 'synthetic_hint') THEN 'shopify_marketing_hint'
            WHEN raw_method_code IN ('ga4_fallback_derived', 'ga4_fallback_match') THEN 'ga4_fallback'
            WHEN raw_method_code = 'unattributed' THEN 'unattributed'
            WHEN raw_source_code IN (
              'landing_session_id',
              'checkout_token',
              'cart_token',
              'customer_identity',
              'stitched_identity_journey',
              'shopify_marketing_hint',
              'shopify_hint_fallback',
              'ga4_fallback',
              'unattributed'
            ) THEN raw_source_code
            WHEN raw_source_code = 'deterministic_shopify_hint' THEN 'shopify_marketing_hint'
            WHEN raw_source_code = 'ga4_fallback_match' THEN 'ga4_fallback'
            ELSE 'unattributed'
          END AS source_code,
          CASE
            WHEN raw_method_code IN (
              'matched_by_landing_session',
              'matched_by_checkout_token',
              'matched_by_cart_token',
              'matched_by_customer_identity',
              'matched_by_identity_journey',
              'shopify_hint_derived',
              'ga4_fallback_derived',
              'ga4_fallback_match',
              'unattributed',
              'synthetic_hint',
              'unknown'
            ) THEN raw_method_code
            WHEN raw_source_code = 'unattributed' THEN 'unattributed'
            ELSE 'unknown'
          END AS method_code
        FROM raw_metadata
      ),
      resolved_metadata AS (
        SELECT
          metadata.*,
          sources.id AS attribution_source_id,
          methods.id AS matching_method_id,
          (
            metadata.source_code = 'unattributed'
            AND COALESCE(metadata.raw_source_code, '') <> 'unattributed'
          )
          OR (
            metadata.method_code = 'unknown'
            AND COALESCE(metadata.raw_method_code, '') <> 'unknown'
          ) AS used_fallback
        FROM normalized_metadata metadata
        JOIN attribution_sources sources
          ON sources.code = metadata.source_code
          AND sources.is_active = true
        JOIN matching_methods methods
          ON methods.code = metadata.method_code
          AND methods.is_active = true
      ),
      order_updates AS (
        UPDATE shopify_orders orders
        SET
          attribution_source_id = metadata.attribution_source_id,
          matching_method_id = metadata.matching_method_id,
          attribution_confidence_score = metadata.confidence_score,
          last_attribution_run_at = metadata.last_attribution_run_at
        FROM resolved_metadata metadata
        WHERE $3::boolean = false
          AND orders.id = metadata.order_row_id
          AND (
            orders.attribution_source_id IS DISTINCT FROM metadata.attribution_source_id
            OR orders.matching_method_id IS DISTINCT FROM metadata.matching_method_id
            OR orders.attribution_confidence_score IS DISTINCT FROM metadata.confidence_score
            OR orders.last_attribution_run_at IS DISTINCT FROM metadata.last_attribution_run_at
          )
        RETURNING orders.id
      ),
      result_updates AS (
        UPDATE attribution_results results
        SET
          attribution_source_id = metadata.attribution_source_id,
          matching_method_id = metadata.matching_method_id,
          last_attribution_run_at = metadata.last_attribution_run_at
        FROM resolved_metadata metadata
        WHERE $3::boolean = false
          AND results.id = metadata.result_id
          AND (
            results.attribution_source_id IS DISTINCT FROM metadata.attribution_source_id
            OR results.matching_method_id IS DISTINCT FROM metadata.matching_method_id
            OR results.last_attribution_run_at IS DISTINCT FROM metadata.last_attribution_run_at
          )
        RETURNING results.id
      )
      SELECT
        (SELECT COUNT(*) FROM candidates)::text AS scanned_orders,
        (SELECT COUNT(*) FROM order_updates)::text AS updated_orders,
        (SELECT COUNT(*) FROM result_updates)::text AS updated_results,
        (SELECT COUNT(*) FROM resolved_metadata WHERE used_fallback)::text AS fallback_rows,
        (SELECT MAX(id)::text FROM candidates) AS last_order_row_id
    `,
    [input.afterOrderRowId ?? '0', input.batchSize, input.dryRun]
  );
  const row = result.rows[0];

  return {
    scannedOrders: Number(row?.scanned_orders ?? '0'),
    updatedOrders: Number(row?.updated_orders ?? '0'),
    updatedResults: Number(row?.updated_results ?? '0'),
    fallbackRows: Number(row?.fallback_rows ?? '0'),
    lastOrderRowId: row?.last_order_row_id ?? null
  };
}

async function executeDefaultBatch(input: ExecuteConfidenceBackfillBatchInput): Promise<ConfidenceBackfillBatchResult> {
  return withTransaction((client) => executeOrderAttributionConfidenceBackfillBatch(client, input));
}

export async function backfillOrderAttributionConfidenceMetadata(
  options: BackfillOrderAttributionConfidenceOptions
): Promise<OrderAttributionConfidenceBackfillReport> {
  const batchSize = normalizePositiveInteger(options.batchSize, DEFAULT_BATCH_SIZE);
  const maxRetries = normalizePositiveInteger(options.maxRetries, DEFAULT_MAX_RETRIES);
  const dryRun = options.dryRun ?? false;
  const progress = parseOrderAttributionConfidenceBackfillProgress(
    options.progress ?? buildEmptyOrderAttributionConfidenceBackfillProgress()
  );
  const executeBatch = options.executeBatch ?? executeDefaultBatch;
  const publishProgress = options.onProgress
    ? async () => options.onProgress?.(parseOrderAttributionConfidenceBackfillProgress(progress))
    : null;

  logInfo('order_attribution_confidence_backfill_started', {
    workerId: options.workerId,
    dryRun,
    batchSize,
    maxRetries,
    afterOrderRowId: progress.cursor.lastOrderRowId
  });

  while (!progress.cursor.completed) {
    let batch: ConfidenceBackfillBatchResult | null = null;

    for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
      try {
        batch = await executeBatch({
          afterOrderRowId: progress.cursor.lastOrderRowId,
          batchSize,
          dryRun
        });
        break;
      } catch (error) {
        logError('order_attribution_confidence_backfill_batch_failed', error, {
          workerId: options.workerId,
          attempt,
          maxRetries,
          afterOrderRowId: progress.cursor.lastOrderRowId
        });

        if (attempt === maxRetries) {
          progress.failedBatches += 1;
          recordFailure(progress.failures, {
            afterOrderRowId: progress.cursor.lastOrderRowId,
            code: normalizeFailureCode(error),
            message: normalizeFailureMessage(error)
          });
          logError('order_attribution_confidence_backfill_failed', error, {
            workerId: options.workerId,
            dryRun,
            batchSize,
            maxRetries,
            scannedOrders: progress.scannedOrders,
            updatedOrders: progress.updatedOrders,
            updatedResults: progress.updatedResults,
            fallbackRows: progress.fallbackRows,
            failedBatches: progress.failedBatches,
            afterOrderRowId: progress.cursor.lastOrderRowId
          });

          if (publishProgress) {
            await publishProgress();
          }

          throw error;
        }
      }
    }

    if (!batch || batch.scannedOrders === 0) {
      progress.cursor.completed = true;

      if (publishProgress) {
        await publishProgress();
      }

      break;
    }

    progress.scannedOrders += batch.scannedOrders;
    progress.updatedOrders += batch.updatedOrders;
    progress.updatedResults += batch.updatedResults;
    progress.fallbackRows += batch.fallbackRows;
    progress.cursor.lastOrderRowId = batch.lastOrderRowId;
    progress.cursor.batchesProcessed += 1;

    logInfo('order_attribution_confidence_backfill_batch_processed', {
      workerId: options.workerId,
      dryRun,
      scannedOrders: batch.scannedOrders,
      updatedOrders: batch.updatedOrders,
      updatedResults: batch.updatedResults,
      fallbackRows: batch.fallbackRows,
      lastOrderRowId: batch.lastOrderRowId,
      batchesProcessed: progress.cursor.batchesProcessed
    });

    if (publishProgress) {
      await publishProgress();
    }
  }

  const report: OrderAttributionConfidenceBackfillReport = {
    ...parseOrderAttributionConfidenceBackfillProgress(progress),
    workerId: options.workerId,
    dryRun,
    batchSize,
    maxRetries
  };

  logInfo(dryRun ? 'order_attribution_confidence_backfill_dry_run_completed' : 'order_attribution_confidence_backfill_completed', {
    ...report,
    documentedFallback:
      'Rows whose legacy attribution source or method cannot be inferred are assigned attribution source unattributed and matching method unknown.'
  });

  return report;
}
