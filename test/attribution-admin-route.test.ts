import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);

async function requestJson(
  server: ReturnType<typeof createServer>,
  path: string,
  input: {
    method?: string;
    headers?: Record<string, string>;
    body?: unknown;
  } = {}
) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    method: input.method ?? 'GET',
    headers: {
      'content-type': 'application/json',
      ...(input.headers ?? {})
    },
    body: input.body === undefined ? undefined : JSON.stringify(input.body)
  });
  const body = await response.json();

  return { response, body };
}

test('order attribution backfill admin route rejects unauthorized requests with the standard admin response', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 401);
    assert.deepEqual(body, {
      error: 'unauthorized',
      message: 'Authentication required'
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route rejects authenticated non-admin users', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return {
      rows: [
        {
          session_id: 7,
          user_id: 42,
          email: 'analyst@example.com',
          display_name: 'Analyst',
          is_admin: false,
          status: 'active',
          last_login_at: new Date('2026-04-25T10:00:00.000Z'),
          created_at: new Date('2026-04-01T00:00:00.000Z'),
          expires_at: new Date('2026-05-01T00:00:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer user-session-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 403);
    assert.deepEqual(body, {
      error: 'forbidden',
      message: 'Admin access required'
    });
    assert.equal(queryCalls, 1);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route validates the shared request contract before enqueueing', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-10',
        endDate: '2026-04-01',
        limit: 6000
      }
    });

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(body.message, 'Invalid order attribution backfill request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.endDate, ['Start date must be on or before end date.']);
    assert.deepEqual(body.details.fieldErrors.limit, ['Limit must be 5000 or less.']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route rejects limit values below the shared minimum', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        limit: 0
      }
    });

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.limit, ['Limit must be greater than 0.']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route enqueues normalized jobs and returns 202 metadata immediately', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 202);
    assert.equal(body.ok, true);
    assert.equal(body.status, 'queued');
    assert.equal(body.submittedBy, 'internal');
    assert.equal(body.options.startDate, '2026-04-01');
    assert.equal(body.options.endDate, '2026-04-05');
    assert.equal(body.options.dryRun, true);
    assert.equal(body.options.limit, 500);
    assert.equal(body.options.webOrdersOnly, true);
    assert.equal(body.options.skipShopifyWriteback, false);
    assert.match(body.jobId, /^[0-9a-f-]{36}$/i);
    assert.match(body.submittedAt, /^\d{4}-\d{2}-\d{2}T/);

    assert.equal(capturedQueries.length, 1);
    assert.match(capturedQueries[0].text, /INSERT INTO order_attribution_backfill_runs/);
    assert.equal(capturedQueries[0].params?.[0], body.jobId);
    assert.equal(capturedQueries[0].params?.[2], 'internal');
    assert.deepEqual(JSON.parse(String(capturedQueries[0].params?.[3])), {
      startDate: '2026-04-01',
      endDate: '2026-04-05',
      dryRun: true,
      limit: 500,
      webOrdersOnly: true,
      skipShopifyWriteback: false
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route preserves explicit options, including max limit and writeback flags', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: false,
        limit: 5000,
        webOrdersOnly: false,
        skipShopifyWriteback: true
      }
    });

    assert.equal(response.status, 202);
    assert.deepEqual(body.options, {
      startDate: '2026-04-01',
      endDate: '2026-04-05',
      dryRun: false,
      limit: 5000,
      webOrdersOnly: false,
      skipShopifyWriteback: true
    });
    assert.equal(capturedQueries.length, 1);
    assert.deepEqual(JSON.parse(String(capturedQueries[0].params?.[3])), {
      startDate: '2026-04-01',
      endDate: '2026-04-05',
      dryRun: false,
      limit: 5000,
      webOrdersOnly: false,
      skipShopifyWriteback: true
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route derives submittedBy from authenticated admin users', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    if (/FROM app_sessions s/.test(text)) {
      return {
        rows: [
          {
            session_id: 9,
            user_id: 73,
            email: 'admin@example.com',
            display_name: 'Admin User',
            is_admin: true,
            status: 'active',
            last_login_at: new Date('2026-04-25T10:00:00.000Z'),
            created_at: new Date('2026-04-01T00:00:00.000Z'),
            expires_at: new Date('2026-05-01T00:00:00.000Z')
          }
        ]
      };
    }

    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer admin-session-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 202);
    assert.equal(body.submittedBy, 'admin@example.com');
    assert.equal(capturedQueries.length, 1);
    assert.equal(capturedQueries[0].params?.[2], 'admin@example.com');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route returns persisted partial reports for failed jobs', async () => {
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

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill/job-failed', {
      headers: {
        authorization: 'Bearer test-reporting-token'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.equal(body.jobId, 'job-failed');
    assert.equal(body.status, 'failed');
    assert.deepEqual(body.report, {
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
    });
    assert.deepEqual(body.error, {
      code: 'DatabaseTimeout',
      message: 'database timeout while scanning orders'
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
