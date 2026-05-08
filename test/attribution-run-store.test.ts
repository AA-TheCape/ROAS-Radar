import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const poolModule = await import('../src/db/pool.js');
const storeModule = await import('../src/modules/attribution/run-store.js');

const { pool } = poolModule;
const {
  AttributionRunConcurrencyError,
  buildAttributionRunConfigHash,
  claimAttributionRuns,
  enqueueAttributionRun,
  getAttributionRun,
  resumeAttributionRun
} = storeModule;

const originalPoolQuery = pool.query.bind(pool);
const originalPoolConnect = pool.connect.bind(pool);

test('buildAttributionRunConfigHash is stable for equivalent requests', () => {
  const left = buildAttributionRunConfigHash({
    submittedBy: 'ops',
    windowStartUtc: '2026-04-01T00:00:00.000Z',
    windowEndUtc: '2026-04-01T23:59:59.999Z',
    scopeKey: 'account:1/day:2026-04-01',
    concurrencyKey: 'account:1/day:2026-04-01',
    batchSize: 250,
    triggerSource: 'manual'
  });
  const right = buildAttributionRunConfigHash({
    submittedBy: 'ignored-different-user',
    windowStartUtc: '2026-04-01T00:00:00.000Z',
    windowEndUtc: '2026-04-01T23:59:59.999Z',
    scopeKey: 'account:1/day:2026-04-01',
    concurrencyKey: 'account:1/day:2026-04-01',
    batchSize: 250,
    triggerSource: 'manual'
  });

  assert.equal(left, right);
});

test('enqueueAttributionRun returns the existing idempotent run when snapshot and config match', async () => {
  let insertCount = 0;

  pool.connect = (async () =>
    ({
      query: async (text: string, params?: unknown[]) => {
        if (text === 'BEGIN' || text === 'COMMIT') {
          return { rows: [] };
        }

        if (text.includes('FROM attribution_runs') && text.includes('idempotency_key')) {
          return {
            rows:
              insertCount === 0
                ? []
                : [
                    {
                      id: 'run-1',
                      attribution_spec_version: 'v1',
                      run_status: 'pending',
                      trigger_source: 'manual',
                      submitted_by: 'ops',
                      scope_key: 'global',
                      concurrency_key: 'global',
                      idempotency_key: 'same-key',
                      started_at_utc: null,
                      completed_at_utc: null,
                      failed_at_utc: null,
                      created_at_utc: new Date('2026-04-30T10:00:00.000Z'),
                      updated_at_utc: new Date('2026-04-30T10:00:00.000Z'),
                      window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
                      window_end_utc: new Date('2026-04-01T23:59:59.999Z'),
                      batch_size: 100,
                      input_snapshot: { orderIds: ['1001'] },
                      input_snapshot_hash: 'a'.repeat(64),
                      run_config_hash: 'b'.repeat(64),
                      run_metadata: {},
                      progress: {},
                      report: null,
                      error_code: null,
                      error_message: null,
                      claimed_by: null,
                      last_heartbeat_at: null,
                      resumed_from_run_id: null
                    }
                  ]
          };
        }

        if (text.includes('INSERT INTO attribution_runs')) {
          insertCount += 1;
          return {
            rows: [
              {
                id: 'run-1',
                attribution_spec_version: 'v1',
                run_status: 'pending',
                trigger_source: 'manual',
                submitted_by: 'ops',
                scope_key: 'global',
                concurrency_key: 'global',
                idempotency_key: 'same-key',
                started_at_utc: null,
                completed_at_utc: null,
                failed_at_utc: null,
                created_at_utc: new Date('2026-04-30T10:00:00.000Z'),
                updated_at_utc: new Date('2026-04-30T10:00:00.000Z'),
                window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
                window_end_utc: new Date('2026-04-01T23:59:59.999Z'),
                batch_size: 100,
                input_snapshot: { orderIds: ['1001'] },
                input_snapshot_hash: 'a'.repeat(64),
                run_config_hash: 'b'.repeat(64),
                run_metadata: {},
                progress: {},
                report: null,
                error_code: null,
                error_message: null,
                claimed_by: null,
                last_heartbeat_at: null,
                resumed_from_run_id: null
              }
            ]
          };
        }

        throw new Error(`Unexpected query: ${text}`);
      },
      release: () => undefined
    })) as typeof pool.connect;

  pool.query = (async (text: string) => {
    if (text.includes('FROM shopify_orders')) {
      return {
        rows: [{ shopify_order_id: '1001' }]
      };
    }

    throw new Error(`Unexpected pool query: ${text}`);
  }) as typeof pool.query;

  try {
    const first = await enqueueAttributionRun({
      submittedBy: 'ops',
      windowStartUtc: '2026-04-01T00:00:00.000Z',
      windowEndUtc: '2026-04-01T23:59:59.999Z',
      idempotencyKey: 'same-key'
    });
    const second = await enqueueAttributionRun({
      submittedBy: 'ops',
      windowStartUtc: '2026-04-01T00:00:00.000Z',
      windowEndUtc: '2026-04-01T23:59:59.999Z',
      idempotencyKey: 'same-key'
    });

    assert.equal(first.id, 'run-1');
    assert.equal(second.id, 'run-1');
    assert.equal(insertCount, 1);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    pool.connect = originalPoolConnect as typeof pool.connect;
  }
});

test('claimAttributionRuns normalizes claimed rows and resets stale failures', async () => {
  const fakeClient = {
    query: async (text: string) => {
      if (text === 'BEGIN' || text === 'COMMIT') {
        return { rows: [] };
      }

      assert.match(text, /UPDATE attribution_runs runs/);
      return {
        rows: [
          {
            id: 'run-claimed',
            attribution_spec_version: 'v1',
            run_status: 'running',
            trigger_source: 'manual',
            submitted_by: 'ops',
            scope_key: 'global',
            concurrency_key: 'global',
            idempotency_key: 'claimed-key',
            started_at_utc: new Date('2026-04-30T10:00:00.000Z'),
            completed_at_utc: null,
            failed_at_utc: null,
            created_at_utc: new Date('2026-04-30T09:55:00.000Z'),
            updated_at_utc: new Date('2026-04-30T10:00:00.000Z'),
            window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
            window_end_utc: new Date('2026-04-01T23:59:59.999Z'),
            batch_size: 100,
            input_snapshot: { orderIds: ['1001', '1002'] },
            input_snapshot_hash: 'c'.repeat(64),
            run_config_hash: 'd'.repeat(64),
            run_metadata: {},
            progress: {},
            report: null,
            error_code: null,
            error_message: null,
            claimed_by: 'worker-1',
            last_heartbeat_at: new Date('2026-04-30T10:00:00.000Z'),
            resumed_from_run_id: null
          }
        ]
      };
    },
    release: () => undefined
  };

  pool.connect = (async () => fakeClient) as typeof pool.connect;

  try {
    const runs = await claimAttributionRuns('worker-1', new Date('2026-04-30T10:00:00.000Z'), 1);
    assert.deepEqual(runs[0].progress, {
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
    });
    assert.equal(runs[0].claimedBy, 'worker-1');
  } finally {
    pool.connect = originalPoolConnect as typeof pool.connect;
  }
});

test('getAttributionRun and resumeAttributionRun map persisted failed runs', async () => {
  let updateSeen = false;

  pool.query = (async (text: string) => {
    if (text.includes('UPDATE attribution_runs') && text.includes('submitted_by')) {
      updateSeen = true;
    }

    return {
      rows: [
        {
          id: 'run-failed',
          attribution_spec_version: 'v1',
          run_status: updateSeen ? 'pending' : 'failed',
          trigger_source: 'manual',
          submitted_by: 'ops',
          scope_key: 'global',
          concurrency_key: 'global',
          idempotency_key: 'failed-key',
          started_at_utc: new Date('2026-04-30T09:00:00.000Z'),
          completed_at_utc: null,
          failed_at_utc: updateSeen ? null : new Date('2026-04-30T09:05:00.000Z'),
          created_at_utc: new Date('2026-04-30T08:55:00.000Z'),
          updated_at_utc: new Date('2026-04-30T09:06:00.000Z'),
          window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
          window_end_utc: new Date('2026-04-01T23:59:59.999Z'),
          batch_size: 100,
          input_snapshot: { orderIds: ['1001'] },
          input_snapshot_hash: 'e'.repeat(64),
          run_config_hash: 'f'.repeat(64),
          run_metadata: {},
          progress: {
            retryOrderIds: ['1001']
          },
          report: {
            failedOrders: 1
          },
          error_code: updateSeen ? null : 'boom',
          error_message: updateSeen ? null : 'failed',
          claimed_by: null,
          last_heartbeat_at: null,
          resumed_from_run_id: null
        }
      ]
    };
  }) as typeof pool.query;

  try {
    const before = await getAttributionRun('run-failed');
    const resumed = await resumeAttributionRun('run-failed', 'ops');

    assert.equal(before?.status, 'failed');
    assert.equal(before?.error?.code, 'boom');
    assert.equal(resumed?.status, 'pending');
    assert.equal(resumed?.error, null);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
  }
});

test('enqueueAttributionRun surfaces active-run concurrency conflicts', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM shopify_orders')) {
      return { rows: [] };
    }

    throw new Error(`Unexpected query: ${text}`);
  }) as typeof pool.query;

  pool.connect = (async () =>
    ({
      query: async (text: string) => {
        if (text === 'BEGIN' || text === 'ROLLBACK') {
          return { rows: [] };
        }

        if (text.includes('FROM attribution_runs') && text.includes('idempotency_key')) {
          return { rows: [] };
        }

        if (text.includes('INSERT INTO attribution_runs')) {
          const error = Object.assign(new Error('duplicate'), {
            code: '23505',
            constraint: 'attribution_runs_active_concurrency_idx'
          });
          throw error;
        }

        throw new Error(`Unexpected query: ${text}`);
      },
      release: () => undefined
    })) as typeof pool.connect;

  try {
    await assert.rejects(
      enqueueAttributionRun({
        submittedBy: 'ops',
        windowStartUtc: '2026-04-01T00:00:00.000Z',
        windowEndUtc: '2026-04-01T23:59:59.999Z'
      }),
      AttributionRunConcurrencyError
    );
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    pool.connect = originalPoolConnect as typeof pool.connect;
  }
});
