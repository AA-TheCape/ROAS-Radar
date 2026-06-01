import assert from 'node:assert/strict';
import test from 'node:test';

import {
  backfillOrderAttributionConfidenceMetadata,
  buildEmptyOrderAttributionConfidenceBackfillProgress,
  parseOrderAttributionConfidenceBackfillProgress
} from '../src/modules/attribution/confidence-backfill.js';

test('parseOrderAttributionConfidenceBackfillProgress normalizes resumable progress', () => {
  assert.deepEqual(parseOrderAttributionConfidenceBackfillProgress({}), buildEmptyOrderAttributionConfidenceBackfillProgress());

  assert.deepEqual(
    parseOrderAttributionConfidenceBackfillProgress({
      scannedOrders: '4',
      updatedOrders: 3,
      updatedResults: 2,
      fallbackRows: 1,
      failedBatches: -10,
      cursor: {
        lastOrderRowId: '42',
        completed: true,
        batchesProcessed: '2'
      }
    }),
    {
      scannedOrders: 4,
      updatedOrders: 3,
      updatedResults: 2,
      fallbackRows: 1,
      failedBatches: 0,
      failures: [],
      cursor: {
        lastOrderRowId: '42',
        completed: true,
        batchesProcessed: 2
      }
    }
  );
});

test('backfillOrderAttributionConfidenceMetadata advances batches and is resumable from progress', async () => {
  const progressEvents: ReturnType<typeof parseOrderAttributionConfidenceBackfillProgress>[] = [];
  const batchInputs: Array<{ afterOrderRowId: string | null; batchSize: number; dryRun: boolean }> = [];

  const report = await backfillOrderAttributionConfidenceMetadata({
    workerId: 'confidence-test-worker',
    batchSize: 2,
    dryRun: true,
    progress: {
      ...buildEmptyOrderAttributionConfidenceBackfillProgress(),
      scannedOrders: 2,
      updatedOrders: 2,
      updatedResults: 1,
      cursor: {
        lastOrderRowId: '10',
        completed: false,
        batchesProcessed: 1
      }
    },
    executeBatch: async (input) => {
      batchInputs.push(input);

      if (input.afterOrderRowId === '10') {
        return {
          scannedOrders: 2,
          updatedOrders: 0,
          updatedResults: 0,
          fallbackRows: 1,
          lastOrderRowId: '12'
        };
      }

      return {
        scannedOrders: 0,
        updatedOrders: 0,
        updatedResults: 0,
        fallbackRows: 0,
        lastOrderRowId: null
      };
    },
    onProgress: (progress) => {
      progressEvents.push(progress);
    }
  });

  assert.deepEqual(batchInputs, [
    {
      afterOrderRowId: '10',
      batchSize: 2,
      dryRun: true
    },
    {
      afterOrderRowId: '12',
      batchSize: 2,
      dryRun: true
    }
  ]);
  assert.equal(report.scannedOrders, 4);
  assert.equal(report.updatedOrders, 2);
  assert.equal(report.updatedResults, 1);
  assert.equal(report.fallbackRows, 1);
  assert.equal(report.cursor.completed, true);
  assert.equal(report.cursor.lastOrderRowId, '12');
  assert.equal(progressEvents.at(-1)?.cursor.completed, true);
});

test('backfillOrderAttributionConfidenceMetadata retries transient batch failures', async () => {
  let attempts = 0;

  const report = await backfillOrderAttributionConfidenceMetadata({
    workerId: 'confidence-retry-worker',
    batchSize: 10,
    maxRetries: 2,
    executeBatch: async () => {
      attempts += 1;

      if (attempts === 1) {
        throw new Error('temporary database timeout');
      }

      return {
        scannedOrders: 0,
        updatedOrders: 0,
        updatedResults: 0,
        fallbackRows: 0,
        lastOrderRowId: null
      };
    }
  });

  assert.equal(attempts, 2);
  assert.equal(report.cursor.completed, true);
  assert.equal(report.failedBatches, 0);
});

test('backfillOrderAttributionConfidenceMetadata records exhausted retry failures', async () => {
  await assert.rejects(
    () =>
      backfillOrderAttributionConfidenceMetadata({
        workerId: 'confidence-failure-worker',
        maxRetries: 2,
        executeBatch: async () => {
          throw new Error('persistent database failure');
        }
      }),
    /persistent database failure/
  );
});
