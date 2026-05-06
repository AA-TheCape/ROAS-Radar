import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const jobsModule = await import('../src/modules/attribution/run-jobs.js');

const { processAttributionRuns } = jobsModule;

test('processAttributionRuns completes successful runs and persists progress-driven reports', async () => {
  const completed: Array<{ runId: string; report: Record<string, unknown> }> = [];

  const result = await processAttributionRuns({
    workerId: 'worker-1',
    claimRuns: async () => [
      {
        id: 'run-1',
        attributionSpecVersion: 'v1',
        status: 'running',
        triggerSource: 'manual',
        submittedBy: 'ops',
        scopeKey: 'global',
        concurrencyKey: 'global',
        idempotencyKey: 'key-1',
        startedAtUtc: null,
        completedAtUtc: null,
        failedAtUtc: null,
        createdAtUtc: '2026-04-30T10:00:00.000Z',
        updatedAtUtc: '2026-04-30T10:00:00.000Z',
        windowStartUtc: '2026-04-01T00:00:00.000Z',
        windowEndUtc: '2026-04-01T23:59:59.999Z',
        batchSize: 100,
        inputSnapshot: { orderIds: ['1001'] },
        inputSnapshotHash: 'a'.repeat(64),
        runConfigHash: 'b'.repeat(64),
        runMetadata: {},
        progress: {
          processedOrders: 0,
          succeededOrders: 0,
          failedOrders: 0,
          retryOrderIds: [],
          lastProcessedOrderId: null,
          cursor: {
            offset: 0,
            completed: false,
            batchesProcessed: 0
          }
        },
        report: null,
        error: null,
        claimedBy: 'worker-1',
        lastHeartbeatAtUtc: null,
        resumedFromRunId: null
      }
    ],
    executeRun: async () => ({
      runId: 'run-1',
      inputSnapshotHash: 'a'.repeat(64),
      orderCount: 1,
      processedOrders: 1,
      succeededOrders: 1,
      failedOrders: 0,
      batchesProcessed: 1,
      retryOrderIdsOutstanding: [],
      lastProcessedOrderId: '1001'
    }),
    markRunCompleted: async (runId, report) => {
      completed.push({ runId, report });
    }
  });

  assert.deepEqual(result, {
    claimedRuns: 1,
    completedRuns: 1,
    failedRuns: 0
  });
  assert.equal(completed[0].runId, 'run-1');
  assert.equal(completed[0].report.processedOrders, 1);
});

test('processAttributionRuns marks runs failed when retryable orders remain outstanding', async () => {
  const failed: Array<{ runId: string; report: Record<string, unknown> | null; code: string }> = [];

  const result = await processAttributionRuns({
    workerId: 'worker-1',
    claimRuns: async () => [
      {
        id: 'run-2',
        attributionSpecVersion: 'v1',
        status: 'running',
        triggerSource: 'manual',
        submittedBy: 'ops',
        scopeKey: 'global',
        concurrencyKey: 'global',
        idempotencyKey: 'key-2',
        startedAtUtc: null,
        completedAtUtc: null,
        failedAtUtc: null,
        createdAtUtc: '2026-04-30T10:00:00.000Z',
        updatedAtUtc: '2026-04-30T10:00:00.000Z',
        windowStartUtc: '2026-04-01T00:00:00.000Z',
        windowEndUtc: '2026-04-01T23:59:59.999Z',
        batchSize: 100,
        inputSnapshot: { orderIds: ['1001'] },
        inputSnapshotHash: 'a'.repeat(64),
        runConfigHash: 'b'.repeat(64),
        runMetadata: {},
        progress: {},
        report: null,
        error: null,
        claimedBy: 'worker-1',
        lastHeartbeatAtUtc: null,
        resumedFromRunId: null
      }
    ],
    executeRun: async () => ({
      runId: 'run-2',
      inputSnapshotHash: 'a'.repeat(64),
      orderCount: 1,
      processedOrders: 1,
      succeededOrders: 0,
      failedOrders: 1,
      batchesProcessed: 1,
      retryOrderIdsOutstanding: ['1001'],
      lastProcessedOrderId: '1001'
    }),
    markRunFailed: async (runId, error, report) => {
      failed.push({
        runId,
        report,
        code: typeof error === 'object' && error !== null && 'code' in error ? String(error.code) : 'unknown'
      });
    }
  });

  assert.deepEqual(result, {
    claimedRuns: 1,
    completedRuns: 0,
    failedRuns: 1
  });
  assert.equal(failed[0].runId, 'run-2');
  assert.equal(failed[0].code, 'attribution_run_retryable_orders_outstanding');
  assert.deepEqual(failed[0].report?.retryOrderIdsOutstanding, ['1001']);
});
