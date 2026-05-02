import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const backfillJobsModule = await import('../src/modules/attribution/backfill-jobs.js');
const backfillModule = await import('../src/modules/attribution/backfill.js');

const { buildBackfillExecutionOptions, processOrderAttributionBackfillRuns } = backfillJobsModule;
const { OrderAttributionBackfillRunError } = backfillModule;

const emptyTierCounts = {
  deterministic_first_party: 0,
  deterministic_shopify_hint: 0,
  platform_reported_meta: 0,
  ga4_fallback: 0,
  unattributed: 0
};

async function captureStructuredLogs<T>(callback: () => Promise<T>): Promise<{
  entries: Array<Record<string, unknown>>;
  result: T;
}> {
  const stdoutChunks: string[] = [];
  const stderrChunks: string[] = [];
  const originalStdoutWrite = process.stdout.write.bind(process.stdout);
  const originalStderrWrite = process.stderr.write.bind(process.stderr);

  process.stdout.write = ((chunk: string | Uint8Array) => {
    stdoutChunks.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
    return true;
  }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: string | Uint8Array) => {
    stderrChunks.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
    return true;
  }) as typeof process.stderr.write;

  try {
    const result = await callback();
    const entries = [...stdoutChunks, ...stderrChunks]
      .join('')
      .trim()
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.startsWith('{') && line.endsWith('}'))
      .map((line) => JSON.parse(line) as Record<string, unknown>);

    return { entries, result };
  } finally {
    process.stdout.write = originalStdoutWrite as typeof process.stdout.write;
    process.stderr.write = originalStderrWrite as typeof process.stderr.write;
  }
}

test('buildBackfillExecutionOptions maps submitted options to backfill execution inputs', () => {
  const executionOptions = buildBackfillExecutionOptions(
    {
      submitted_by: 'operator@roasradar.dev',
      options: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: false,
        limit: 275,
        reclassificationTarget: 'full_rebuild',
        organizationIds: [],
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
  assert.equal(executionOptions.reclassificationTarget, 'full_rebuild');
  assert.deepEqual(executionOptions.organizationIds, []);
  assert.equal(executionOptions.onlyWebOrders, false);
  assert.equal(executionOptions.writeToShopifyWhenAvailable, false);
});

test('buildBackfillExecutionOptions keeps writeback enabled for non-dry-run web-only batches when skipShopifyWriteback is false', () => {
  const executionOptions = buildBackfillExecutionOptions(
    {
      submitted_by: 'operator@roasradar.dev',
      options: {
        startDate: '2026-04-06',
        endDate: '2026-04-08',
        dryRun: false,
        limit: 5000,
        reclassificationTarget: 'meta_tier_reclassification',
        organizationIds: [17, 29],
        webOrdersOnly: true,
        skipShopifyWriteback: false
      }
    },
    'worker-456'
  );

  assert.equal(executionOptions.requestedBy, 'operator@roasradar.dev');
  assert.equal(executionOptions.workerId, 'worker-456');
  assert.equal(executionOptions.limit, 5000);
  assert.equal(executionOptions.dryRun, false);
  assert.equal(executionOptions.reclassificationTarget, 'meta_tier_reclassification');
  assert.deepEqual(executionOptions.organizationIds, [17, 29]);
  assert.equal(executionOptions.onlyWebOrders, true);
  assert.equal(executionOptions.writeToShopifyWhenAvailable, true);
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
          reclassificationTarget: 'meta_tier_reclassification',
          organizationIds: [88],
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
        reclassificationTarget: options.reclassificationTarget,
        organizationIds: options.organizationIds,
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
          limit: 42,
          reclassificationTarget: 'meta_tier_reclassification',
          organizationIds: [88]
        },
        beforeMetrics: {
          totalOrdersInScope: 40,
          ordersMissingAttribution: 17,
          ordersWithAttribution: 23,
          completenessRate: 0.575,
          tierCounts: emptyTierCounts
        },
        afterMetrics: {
          totalOrdersInScope: 40,
          ordersMissingAttribution: 6,
          ordersWithAttribution: 34,
          completenessRate: 0.85,
          tierCounts: emptyTierCounts
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
      reclassificationTarget: 'meta_tier_reclassification',
      organizationIds: [88],
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
        dryRun: true,
        reclassificationTarget: 'meta_tier_reclassification',
        organizationIds: [88],
        beforeCounts: emptyTierCounts,
        afterCounts: emptyTierCounts,
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

test('processOrderAttributionBackfillRuns continues after a failure and preserves explicit execution flags per run', async () => {
  const capturedExecutionOptions: Array<Record<string, unknown>> = [];
  const completedRuns: Array<{ runId: string; report: unknown }> = [];
  const failedRuns: Array<{ runId: string; report: unknown; error: { code: string; message: string } }> = [];

  const result = await processOrderAttributionBackfillRuns({
    workerId: 'worker-runner',
    claimRuns: async () => [
      {
        id: 'job-success',
        submitted_by: 'internal',
        options: {
          startDate: '2026-04-10',
          endDate: '2026-04-12',
          dryRun: false,
          limit: 5000,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          webOrdersOnly: false,
          skipShopifyWriteback: true
        }
      },
      {
        id: 'job-failed',
        submitted_by: 'internal',
        options: {
          startDate: '2026-04-13',
          endDate: '2026-04-13',
          dryRun: true,
          limit: 10,
          reclassificationTarget: 'meta_tier_reclassification',
          organizationIds: [41],
          webOrdersOnly: true,
          skipShopifyWriteback: false
        }
      }
    ],
    markRunCompleted: async (runId, report) => {
      completedRuns.push({ runId, report });
    },
    markRunFailed: async (runId, error, report) => {
      failedRuns.push({
        runId,
        report,
        error: {
          code: error instanceof Error ? error.name : 'unknown_error',
          message: error instanceof Error ? error.message : String(error)
        }
      });
    },
    executeBackfillRun: async (options) => {
      capturedExecutionOptions.push({
        requestedBy: options.requestedBy,
        workerId: options.workerId,
        limit: options.limit,
        dryRun: options.dryRun,
        reclassificationTarget: options.reclassificationTarget,
        organizationIds: options.organizationIds,
        onlyWebOrders: options.onlyWebOrders,
        writeToShopifyWhenAvailable: options.writeToShopifyWhenAvailable
      });

      if (options.limit === 5000) {
        return {
          requestedBy: options.requestedBy,
          workerId: options.workerId,
          dryRun: false,
          scope: {
            windowStart: options.windowStart.toISOString(),
            windowEnd: options.windowEnd.toISOString(),
            onlyWebOrders: false,
            limit: 5000,
            reclassificationTarget: 'full_rebuild',
            organizationIds: []
          },
          beforeMetrics: {
            totalOrdersInScope: 100,
            ordersMissingAttribution: 18,
            ordersWithAttribution: 82,
            completenessRate: 0.82,
            tierCounts: emptyTierCounts
          },
          afterMetrics: {
            totalOrdersInScope: 100,
            ordersMissingAttribution: 9,
            ordersWithAttribution: 91,
            completenessRate: 0.91,
            tierCounts: emptyTierCounts
          },
          scannedOrders: 100,
          recoverableOrders: 9,
          recoveredOrders: 9,
          unrecoverableOrders: 9,
          failedOrders: 0,
          shopifyWritebackCompleted: 0,
          shopifyWritebackSkipped: 9,
          shopifyWritebackFailed: 0,
          failures: [],
          preview: []
        };
      }

      throw new OrderAttributionBackfillRunError('shopify api timeout', {
        code: 'ShopifyTimeout',
        report: {
          scanned: 3,
          recovered: 0,
          unrecoverable: 1,
          writebackCompleted: 0,
          failures: [
            {
              orderId: 'order-22',
              code: 'shopify_timeout',
              message: 'Shopify API timed out while checking writeback state'
            }
          ]
        }
      });
    }
  });

  assert.deepEqual(result, {
    claimedRuns: 2,
    completedRuns: 1,
    failedRuns: 1
  });
  assert.deepEqual(capturedExecutionOptions, [
    {
      requestedBy: 'internal',
      workerId: 'worker-runner',
        limit: 5000,
        dryRun: false,
        reclassificationTarget: 'full_rebuild',
        organizationIds: [],
        onlyWebOrders: false,
        writeToShopifyWhenAvailable: false
      },
    {
      requestedBy: 'internal',
      workerId: 'worker-runner',
        limit: 10,
        dryRun: true,
        reclassificationTarget: 'meta_tier_reclassification',
        organizationIds: [41],
        onlyWebOrders: true,
        writeToShopifyWhenAvailable: true
      }
  ]);
  assert.deepEqual(completedRuns, [
    {
      runId: 'job-success',
      report: {
        scanned: 100,
        recovered: 9,
        unrecoverable: 9,
        writebackCompleted: 0,
        dryRun: false,
        reclassificationTarget: 'full_rebuild',
        organizationIds: [],
        beforeCounts: emptyTierCounts,
        afterCounts: emptyTierCounts,
        failures: []
      }
    }
  ]);
  assert.deepEqual(failedRuns, [
    {
      runId: 'job-failed',
        report: {
          scanned: 3,
          recovered: 0,
          unrecoverable: 1,
          writebackCompleted: 0,
          dryRun: false,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          beforeCounts: emptyTierCounts,
          afterCounts: emptyTierCounts,
          failures: [
          {
            orderId: 'order-22',
            code: 'shopify_timeout',
            message: 'Shopify API timed out while checking writeback state'
          }
        ]
      },
      error: {
        code: 'OrderAttributionBackfillRunError',
        message: 'shopify api timeout'
      }
    }
  ]);
});

test('processOrderAttributionBackfillRuns marks failed runs without aborting the batch', async () => {
  const failedRuns: Array<{ runId: string; report: unknown; error: { code: string; message: string } }> = [];

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
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          webOrdersOnly: true,
          skipShopifyWriteback: true
        }
      }
    ],
    markRunFailed: async (runId, error, report) => {
      failedRuns.push({
        runId,
        report,
        error: {
          code: error instanceof Error ? error.name : 'unknown_error',
          message: error instanceof Error ? error.message : String(error)
        }
      });
    },
    executeBackfillRun: async () => {
      const error = new Error('database timeout while scanning orders');
      error.name = 'DatabaseTimeout';
      throw new OrderAttributionBackfillRunError(error.message, {
        code: 'DatabaseTimeout',
        report: {
          scanned: 0,
          recovered: 0,
          unrecoverable: 0,
          writebackCompleted: 0,
          dryRun: false,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          beforeCounts: emptyTierCounts,
          afterCounts: emptyTierCounts,
          failures: []
        },
        cause: error
      });
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
      report: {
        scanned: 0,
        recovered: 0,
        unrecoverable: 0,
        writebackCompleted: 0,
        dryRun: false,
        reclassificationTarget: 'full_rebuild',
        organizationIds: [],
        beforeCounts: emptyTierCounts,
        afterCounts: emptyTierCounts,
        failures: []
      },
      error: {
        code: 'OrderAttributionBackfillRunError',
        message: 'database timeout while scanning orders'
      }
    }
  ]);
});

test('processOrderAttributionBackfillRuns emits per-job lifecycle logs with job ids and failure summaries', async () => {
  const { entries, result } = await captureStructuredLogs(() =>
    processOrderAttributionBackfillRuns({
      workerId: 'worker-observed',
      claimRuns: async () => [
        {
          id: 'job-complete',
          submitted_by: 'internal',
          options: {
            startDate: '2026-04-20',
            endDate: '2026-04-21',
            dryRun: true,
            limit: 25,
            reclassificationTarget: 'full_rebuild',
            organizationIds: [],
            webOrdersOnly: true,
            skipShopifyWriteback: false
          }
        },
        {
          id: 'job-failed',
          submitted_by: 'internal',
          options: {
            startDate: '2026-04-22',
            endDate: '2026-04-22',
            dryRun: false,
            limit: 10,
            reclassificationTarget: 'meta_tier_reclassification',
            organizationIds: [73],
            webOrdersOnly: false,
            skipShopifyWriteback: true
          }
        }
      ],
      markRunCompleted: async () => undefined,
      markRunFailed: async () => undefined,
      executeBackfillRun: async (options) => {
        if (options.limit === 25) {
          return {
            requestedBy: options.requestedBy,
            workerId: options.workerId,
            dryRun: true,
            scope: {
              windowStart: options.windowStart.toISOString(),
              windowEnd: options.windowEnd.toISOString(),
              onlyWebOrders: options.onlyWebOrders ?? true,
              limit: options.limit ?? 25,
              reclassificationTarget: 'full_rebuild',
              organizationIds: []
            },
            beforeMetrics: {
              totalOrdersInScope: 25,
              ordersMissingAttribution: 9,
              ordersWithAttribution: 16,
              completenessRate: 0.64,
              tierCounts: emptyTierCounts
            },
            afterMetrics: {
              totalOrdersInScope: 25,
              ordersMissingAttribution: 9,
              ordersWithAttribution: 16,
              completenessRate: 0.64,
              tierCounts: emptyTierCounts
            },
            scannedOrders: 25,
            recoverableOrders: 7,
            recoveredOrders: 0,
            unrecoverableOrders: 2,
            failedOrders: 0,
            shopifyWritebackCompleted: 0,
            shopifyWritebackSkipped: 0,
            shopifyWritebackFailed: 0,
            failures: [],
            preview: []
          };
        }

        throw new OrderAttributionBackfillRunError('Worker failed while writing report', {
          code: 'report_write_failed',
          report: {
            scanned: 10,
            recovered: 2,
            unrecoverable: 3,
            writebackCompleted: 1,
            dryRun: false,
            reclassificationTarget: 'full_rebuild',
            organizationIds: [],
            beforeCounts: emptyTierCounts,
            afterCounts: emptyTierCounts,
            failures: [
              {
                orderId: '1003',
                code: 'shopify_writeback_failed',
                message: 'Shopify writeback failed for order 1003'
              }
            ]
          }
        });
      }
    })
  );

  assert.deepEqual(result, {
    claimedRuns: 2,
    completedRuns: 1,
    failedRuns: 1
  });

  const lifecycleEntries = entries.filter((entry) => entry.event === 'order_attribution_backfill_job_lifecycle');
  assert.equal(lifecycleEntries.length, 4);
  assert.deepEqual(
    lifecycleEntries.map((entry) => ({
      jobId: entry.jobId,
      stage: entry.stage,
      status: entry.status
    })),
    [
      {
        jobId: 'job-complete',
        stage: 'started',
        status: 'processing'
      },
      {
        jobId: 'job-complete',
        stage: 'completed',
        status: 'completed'
      },
      {
        jobId: 'job-failed',
        stage: 'started',
        status: 'processing'
      },
      {
        jobId: 'job-failed',
        stage: 'failed',
        status: 'failed'
      }
    ]
  );
  assert.deepEqual(lifecycleEntries[1].report, {
    scanned: 25,
    recovered: 0,
    unrecoverable: 2,
    writebackCompleted: 0,
    failures: [],
    failureCount: 0,
    sampleFailures: []
  });
  assert.equal(lifecycleEntries[3].code, 'report_write_failed');
  assert.deepEqual(lifecycleEntries[3].report, {
    scanned: 10,
    recovered: 2,
    unrecoverable: 3,
    writebackCompleted: 1,
    failures: [
      {
        orderId: '1003',
        code: 'shopify_writeback_failed',
        message: 'Shopify writeback failed for order 1003'
      }
    ],
    failureCount: 1,
    sampleFailures: [
      {
        orderId: '1003',
        code: 'shopify_writeback_failed',
        message: 'Shopify writeback failed for order 1003'
      }
    ]
  });
});
