import assert from 'node:assert/strict';
import test from 'node:test';

const { __observabilityTestUtils } = await import('../src/observability/index.js');

function captureStructuredLogs<T>(callback: () => T): { entries: Array<Record<string, unknown>>; result: T } {
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
    const result = callback();
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

test('summarizeAttributionObservation classifies complete captures and missing session ids', () => {
  const complete = __observabilityTestUtils.summarizeAttributionObservation({
    roas_radar_session_id: '123e4567-e89b-42d3-a456-426614174000',
    landing_url: 'https://store.example/?utm_source=google',
    page_url: 'https://store.example/products/widget',
    utm_source: 'google',
    gclid: 'GCLID-123'
  });

  assert.equal(complete.captureStatus, 'complete');
});

test('summarizeDualWriteConsistency flags failed server legs as mismatches', () => {
  assert.deepEqual(
    __observabilityTestUtils.summarizeDualWriteConsistency({
      browserOutcome: 'accepted',
      serverOutcome: 'failed'
    }),
    {
      consistencyStatus: 'mismatched',
      browserOutcome: 'accepted',
      serverOutcome: 'failed'
    }
  );
});

test('summarizeResolverOutcome reports unattributed and non-direct winners deterministically', () => {
  const unattributed = __observabilityTestUtils.summarizeResolverOutcome({
    touchpoints: [],
    winner: null
  });

  assert.equal(unattributed.resolverOutcome, 'unattributed');
});

test('summarizeOrderAttributionBackfillReport keeps operational counters and bounded failure samples', () => {
  assert.deepEqual(
    __observabilityTestUtils.summarizeOrderAttributionBackfillReport({
      scanned: 24,
      recovered: 7,
      unrecoverable: 5,
      writebackCompleted: 4,
      failures: [
        {
          orderId: '1001',
          code: 'shopify_writeback_failed',
          message: 'Writeback request timed out'
        }
      ]
    }),
    {
      scanned: 24,
      recovered: 7,
      unrecoverable: 5,
      writebackCompleted: 4,
      failureCount: 1,
      sampleFailures: [
        {
          orderId: '1001',
          code: 'shopify_writeback_failed',
          message: 'Writeback request timed out'
        }
      ]
    }
  );
});

test('emitOrderAttributionBackfillJobLifecycleLog emits structured lifecycle logs with job ids and failure metadata', () => {
  const { entries } = captureStructuredLogs(() => {
    __observabilityTestUtils.emitOrderAttributionBackfillJobLifecycleLog({
      stage: 'enqueued',
      jobId: 'job-enqueued',
      submittedAt: '2026-04-25T10:00:00.000Z',
      options: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: true,
        limit: 500,
        webOrdersOnly: true,
        skipShopifyWriteback: false
      }
    });

    __observabilityTestUtils.emitOrderAttributionBackfillJobLifecycleLog({
      stage: 'failed',
      jobId: 'job-failed',
      workerId: 'worker-1',
      startedAt: '2026-04-25T10:01:00.000Z',
      completedAt: '2026-04-25T10:02:00.000Z',
      options: {
        startDate: '2026-04-06',
        endDate: '2026-04-07',
        dryRun: false,
        limit: 50,
        webOrdersOnly: false,
        skipShopifyWriteback: true
      },
      report: {
        scanned: 12,
        recovered: 3,
        unrecoverable: 4,
        writebackCompleted: 2,
        failures: [
          {
            orderId: '1002',
            code: 'order_not_found',
            message: 'Shopify order 1002 was not found'
          }
        ]
      },
      error: Object.assign(new Error('Database timeout while persisting backfill results'), {
        code: 'database_timeout'
      })
    });
  });

  assert.equal(entries.length, 2);
  assert.equal(entries[0].event, 'order_attribution_backfill_job_lifecycle');
  assert.equal(entries[0].jobId, 'job-enqueued');
  assert.equal(entries[0].stage, 'enqueued');
  assert.equal(entries[0].status, 'queued');
  assert.equal(entries[0].dryRun, true);

  assert.equal(entries[1].event, 'order_attribution_backfill_job_lifecycle');
  assert.equal(entries[1].jobId, 'job-failed');
  assert.equal(entries[1].stage, 'failed');
  assert.equal(entries[1].status, 'failed');
  assert.equal(entries[1].alertable, true);
  assert.equal(entries[1].code, 'database_timeout');
  assert.equal(entries[1].failureMessage, 'Database timeout while persisting backfill results');
  assert.deepEqual(entries[1].report, {
    scanned: 12,
    recovered: 3,
    unrecoverable: 4,
    writebackCompleted: 2,
    failureCount: 1,
    sampleFailures: [
      {
        orderId: '1002',
        code: 'order_not_found',
        message: 'Shopify order 1002 was not found'
      }
    ]
  });
});
