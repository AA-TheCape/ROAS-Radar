import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const backfillJobsModule = await import('../src/modules/attribution/backfill-jobs.js');

const { buildBackfillExecutionOptions, processOrderAttributionBackfillRuns } = backfillJobsModule;

test('buildBackfillExecutionOptions maps submitted options to backfill execution inputs', () => {
  const executionOptions = buildBackfillExecutionOptions(
    {
      submitted_by: 'operator@roasradar.dev',
      options: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: false,
        limit: 275,
        webOrdersOnly: false,
        skipShopifyWriteback: true
      }
    },
    'worker-123'
  );

  assert.equal(executionOptions.requestedBy, 'operator@roasradar.dev');
  assert.equal(executionOptions.workerId, 'worker-123');
  assert.equal(executionOptions.windowStart.toISOString(), '2026-04-01T00:00:00.000Z');
  assert.equal(executionOptions.windowEnd.toISOString(), '2026-04-05T23:59:59.999Z');
  assert.equal(executionOptions.limit, 275);
  assert.equal(executionOptions.dryRun, false);
  assert.equal(executionOptions.onlyWebOrders, false);
  assert.equal(executionOptions.writeToShopifyWhenAvailable, false);
});

test('processOrderAttributionBackfillRuns completes queued runs with the shared final report shape', async () => {
  const capturedExecutionOptions: Array<Record<string, unknown>> = [];
  const completedRuns: Array<{ runId: string; report: unknown }> = [];

  const result = await processOrderAttributionBackfillRuns({
    workerId: 'worker-runner',
    claimRuns: async () => [
      {
        id: 'job-1',
        submitted_by: 'internal',
        options: {
          startDate: '2026-04-10',
          endDate: '2026-04-12',
          dryRun: true,
          limit: 42,
          webOrdersOnly: true,
          skipShopifyWriteback: false
        }
      }
    ],
    markRunCompleted: async (runId, report) => {
      completedRuns.push({ runId, report });
    },
    executeBackfillRun: async (options) => {
      capturedExecutionOptions.push({
        requestedBy: options.requestedBy,
        workerId: options.workerId,
        windowStart: options.windowStart.toISOString(),
        windowEnd: options.windowEnd.toISOString(),
        limit: options.limit,
        dryRun: options.dryRun,
        onlyWebOrders: options.onlyWebOrders,
        writeToShopifyWhenAvailable: options.writeToShopifyWhenAvailable
      });

      return {
        requestedBy: options.requestedBy,
        workerId: options.workerId,
        dryRun: true,
        scope: {
          windowStart: options.windowStart.toISOString(),
          windowEnd: options.windowEnd.toISOString(),
          onlyWebOrders: true,
          limit: 42
        },
        beforeMetrics: {
          totalOrdersInScope: 40,
          ordersMissingAttribution: 17,
          ordersWithAttribution: 23,
          completenessRate: 0.575
        },
        afterMetrics: {
          totalOrdersInScope: 40,
          ordersMissingAttribution: 6,
          ordersWithAttribution: 34,
          completenessRate: 0.85
        },
        scannedOrders: 40,
        recoverableOrders: 11,
        recoveredOrders: 11,
        unrecoverableOrders: 6,
        failedOrders: 0,
        shopifyWritebackCompleted: 3,
        shopifyWritebackSkipped: 8,
        shopifyWritebackFailed: 1,
        failures: [
          {
            orderId: 'order-7',
            code: 'shopify_writeback_failed',
            message: 'Shopify writeback failed for order-7'
          }
        ],
        preview: []
      };
    }
  });

  assert.deepEqual(result, {
    claimedRuns: 1,
    completedRuns: 1,
    failedRuns: 0
  });
  assert.deepEqual(capturedExecutionOptions, [
    {
      requestedBy: 'internal',
      workerId: 'worker-runner',
      windowStart: '2026-04-10T00:00:00.000Z',
      windowEnd: '2026-04-12T23:59:59.999Z',
      limit: 42,
      dryRun: true,
      onlyWebOrders: true,
      writeToShopifyWhenAvailable: true
    }
  ]);
  assert.deepEqual(completedRuns, [
    {
      runId: 'job-1',
      report: {
        scanned: 40,
        recovered: 11,
        unrecoverable: 6,
        writebackCompleted: 3,
        failures: [
          {
            orderId: 'order-7',
            code: 'shopify_writeback_failed',
            message: 'Shopify writeback failed for order-7'
          }
        ]
      }
    }
  ]);
});

test('processOrderAttributionBackfillRuns marks failed runs without aborting the batch', async () => {
  const failedRuns: Array<{ runId: string; error: { code: string; message: string } }> = [];

  const result = await processOrderAttributionBackfillRuns({
    workerId: 'worker-runner',
    claimRuns: async () => [
      {
        id: 'job-failed',
        submitted_by: 'internal',
        options: {
          startDate: '2026-04-01',
          endDate: '2026-04-01',
          dryRun: false,
          limit: 10,
          webOrdersOnly: true,
          skipShopifyWriteback: true
        }
      }
    ],
    markRunFailed: async (runId, error) => {
      failedRuns.push({
        runId,
        error: {
          code: error instanceof Error ? error.name : 'unknown_error',
          message: error instanceof Error ? error.message : String(error)
        }
      });
    },
    executeBackfillRun: async () => {
      const error = new Error('database timeout while scanning orders');
      error.name = 'DatabaseTimeout';
      throw error;
    }
  });

  assert.deepEqual(result, {
    claimedRuns: 1,
    completedRuns: 0,
    failedRuns: 1
  });
  assert.deepEqual(failedRuns, [
    {
      runId: 'job-failed',
      error: {
        code: 'DatabaseTimeout',
        message: 'database timeout while scanning orders'
      }
    }
  ]);
});
