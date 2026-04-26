import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const poolModule = await import('../src/db/pool.js');
const storeModule = await import('../src/modules/attribution/backfill-run-store.js');

const { pool } = poolModule;
const {
  claimOrderAttributionBackfillRuns,
  enqueueOrderAttributionBackfillRun,
  getOrderAttributionBackfillRun,
  markOrderAttributionBackfillRunCompleted,
  markOrderAttributionBackfillRunFailed
} = storeModule;

const originalPoolQuery = pool.query.bind(pool);
const originalPoolConnect = pool.connect.bind(pool);

test('enqueueOrderAttributionBackfillRun persists normalized options and returns the shared enqueue shape', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  try {
    const response = await enqueueOrderAttributionBackfillRun(
      {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: true,
        limit: 500,
        webOrdersOnly: true,
        skipShopifyWriteback: false
      },
      'internal',
      new Date('2026-04-25T12:00:00.000Z')
    );

    assert.equal(response.ok, true);
    assert.equal(response.status, 'queued');
    assert.equal(response.submittedAt, '2026-04-25T12:00:00.000Z');
    assert.equal(response.submittedBy, 'internal');
    assert.deepEqual(response.options, {
      startDate: '2026-04-01',
      endDate: '2026-04-05',
      dryRun: true,
      limit: 500,
      webOrdersOnly: true,
      skipShopifyWriteback: false
    });

    assert.equal(capturedQueries.length, 1);
    assert.match(capturedQueries[0].text, /INSERT INTO order_attribution_backfill_runs/);
    assert.deepEqual(JSON.parse(String(capturedQueries[0].params?.[3])), response.options);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
  }
});

test('claimOrderAttributionBackfillRuns returns normalized claimed runs and resets stale terminal fields', async () => {
  const clientCalls: Array<{ text: string; params?: unknown[] }> = [];
  const fakeClient = {
    query: async (text: string, params?: unknown[]) => {
      clientCalls.push({ text, params });

      if (text === 'BEGIN' || text === 'COMMIT') {
        return { rows: [] };
      }

      assert.match(text, /UPDATE order_attribution_backfill_runs runs/);
      return {
        rows: [
          {
            id: 'job-1',
            status: 'processing',
            submitted_at: new Date('2026-04-25T11:55:00.000Z'),
            submitted_by: 'internal',
            started_at: new Date('2026-04-25T12:10:00.000Z'),
            completed_at: null,
            options: {
              startDate: '2026-04-01',
              endDate: '2026-04-05',
              dryRun: true,
              limit: 500,
              webOrdersOnly: true,
              skipShopifyWriteback: false
            },
            report: null,
            error_code: null,
            error_message: null
          }
        ]
      };
    },
    release: () => undefined
  };

  pool.connect = (async () => fakeClient) as typeof pool.connect;

  try {
    const claimedRows = await claimOrderAttributionBackfillRuns('worker-1', new Date('2026-04-25T12:10:00.000Z'), 2);

    assert.deepEqual(claimedRows, [
      {
        id: 'job-1',
        submittedBy: 'internal',
        submittedAt: '2026-04-25T11:55:00.000Z',
        startedAt: '2026-04-25T12:10:00.000Z',
        options: {
          startDate: '2026-04-01',
          endDate: '2026-04-05',
          dryRun: true,
          limit: 500,
          webOrdersOnly: true,
          skipShopifyWriteback: false
        }
      }
    ]);
    assert.equal(clientCalls.length, 3);
    assert.equal(clientCalls[0].text, 'BEGIN');
    assert.match(clientCalls[1].text, /completed_at = NULL/);
    assert.equal(clientCalls[2].text, 'COMMIT');
  } finally {
    pool.connect = originalPoolConnect as typeof pool.connect;
  }
});

test('getOrderAttributionBackfillRun maps persisted failure details into the shared job response', async () => {
  pool.query = (async () => ({
    rows: [
      {
        id: 'job-failed',
        status: 'failed',
        submitted_at: new Date('2026-04-25T10:00:00.000Z'),
        submitted_by: 'internal',
        started_at: new Date('2026-04-25T10:00:05.000Z'),
        completed_at: new Date('2026-04-25T10:01:00.000Z'),
        options: {
          startDate: '2026-04-01',
          endDate: '2026-04-05',
          dryRun: false,
          limit: 500,
          webOrdersOnly: true,
          skipShopifyWriteback: false
        },
        report: {
          scanned: 12,
          recovered: 4,
          unrecoverable: 3,
          writebackCompleted: 2,
          failures: [
            {
              orderId: 'order-9',
              code: 'order_processing_failed',
              message: 'Timed out while refreshing daily reporting metrics'
            }
          ]
        },
        error_code: 'DatabaseTimeout',
        error_message: 'database timeout while scanning orders'
      }
    ]
  })) as typeof pool.query;

  try {
    const response = await getOrderAttributionBackfillRun('job-failed');

    assert.deepEqual(response, {
      ok: true,
      jobId: 'job-failed',
      status: 'failed',
      submittedAt: '2026-04-25T10:00:00.000Z',
      submittedBy: 'internal',
      startedAt: '2026-04-25T10:00:05.000Z',
      completedAt: '2026-04-25T10:01:00.000Z',
      options: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: false,
        limit: 500,
        webOrdersOnly: true,
        skipShopifyWriteback: false
      },
      report: {
        scanned: 12,
        recovered: 4,
        unrecoverable: 3,
        writebackCompleted: 2,
        failures: [
          {
            orderId: 'order-9',
            code: 'order_processing_failed',
            message: 'Timed out while refreshing daily reporting metrics'
          }
        ]
      },
      error: {
        code: 'DatabaseTimeout',
        message: 'database timeout while scanning orders'
      }
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
  }
});

test.after(() => {
  pool.query = originalPoolQuery as typeof pool.query;
  pool.connect = originalPoolConnect as typeof pool.connect;
});

test('markOrderAttributionBackfillRunCompleted and markOrderAttributionBackfillRunFailed persist terminal payloads', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  try {
    await markOrderAttributionBackfillRunCompleted(
      'job-complete',
      {
        scanned: 10,
        recovered: 2,
        unrecoverable: 3,
        writebackCompleted: 1,
        failures: []
      },
      new Date('2026-04-25T12:20:00.000Z')
    );
    await markOrderAttributionBackfillRunFailed(
      'job-failed',
      new Error('database timeout'),
      {
        scanned: 3,
        recovered: 0,
        unrecoverable: 1,
        writebackCompleted: 0,
        failures: []
      },
      new Date('2026-04-25T12:25:00.000Z')
    );

    assert.equal(capturedQueries.length, 2);
    assert.match(capturedQueries[0].text, /status = 'completed'/);
    assert.deepEqual(JSON.parse(String(capturedQueries[0].params?.[2])), {
      scanned: 10,
      recovered: 2,
      unrecoverable: 3,
      writebackCompleted: 1,
      failures: []
    });
    assert.match(capturedQueries[1].text, /status = 'failed'/);
    assert.deepEqual(JSON.parse(String(capturedQueries[1].params?.[2])), {
      scanned: 3,
      recovered: 0,
      unrecoverable: 1,
      writebackCompleted: 0,
      failures: []
    });
    assert.equal(capturedQueries[1].params?.[3], 'Error');
    assert.equal(capturedQueries[1].params?.[4], 'database timeout');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
  }
});
