import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN ??= 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);
const originalReportingApiToken = process.env.REPORTING_API_TOKEN;
const originalApiAllowedOrigins = process.env.API_ALLOWED_ORIGINS;

async function request(server: ReturnType<typeof createServer>, path: string, init: RequestInit = {}) {
  const address = server.address() as AddressInfo;

  return fetch(`http://127.0.0.1:${address.port}${path}`, init);
}

function restoreEnv() {
  if (originalReportingApiToken === undefined) {
    delete process.env.REPORTING_API_TOKEN;
  } else {
    process.env.REPORTING_API_TOKEN = originalReportingApiToken;
  }

  if (originalApiAllowedOrigins === undefined) {
    delete process.env.API_ALLOWED_ORIGINS;
  } else {
    process.env.API_ALLOWED_ORIGINS = originalApiAllowedOrigins;
  }
}

test('internal identity routes fail closed when the reporting token is blank', async () => {
  process.env.REPORTING_API_TOKEN = '   ';

  pool.query = (async (text: string) => {
    assert.match(text, /FROM app_sessions s/);
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const response = await request(server, '/api/internal/identity/journeys/11111111-1111-4111-8111-111111111111', {
      headers: {
        authorization: 'Bearer test-reporting-token'
      }
    });
    const body = await response.json();

    assert.equal(response.status, 401);
    assert.equal(body.error, 'unauthorized');
  } finally {
    restoreEnv();
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('admin routes do not accept the legacy default reporting token when no token is configured', async () => {
  process.env.REPORTING_API_TOKEN = '';

  pool.query = (async (text: string) => {
    assert.match(text, /FROM app_sessions s/);
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const response = await request(server, '/api/admin/users', {
      headers: {
        authorization: 'Bearer dev-reporting-token'
      }
    });
    const body = await response.json();

    assert.equal(response.status, 401);
    assert.equal(body.error, 'unauthorized');
  } finally {
    restoreEnv();
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('api cors reflects only explicitly allowed origins', async () => {
  process.env.API_ALLOWED_ORIGINS = 'https://dashboard.example.com';

  const server = createServer();

  try {
    const response = await request(server, '/api/reporting/summary', {
      method: 'OPTIONS',
      headers: {
        origin: 'https://dashboard.example.com',
        'access-control-request-method': 'GET'
      }
    });

    assert.equal(response.status, 204);
    assert.equal(response.headers.get('access-control-allow-origin'), 'https://dashboard.example.com');
    assert.equal(response.headers.get('vary'), 'Origin');
    assert.equal(response.headers.get('access-control-allow-methods'), 'GET,POST,OPTIONS');
  } finally {
    restoreEnv();
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('api cors rejects disallowed preflight origins without reflecting allow-origin', async () => {
  process.env.API_ALLOWED_ORIGINS = 'https://dashboard.example.com';

  const server = createServer();

  try {
    const response = await request(server, '/api/reporting/summary', {
      method: 'OPTIONS',
      headers: {
        origin: 'https://evil.example.com',
        'access-control-request-method': 'GET'
      }
    });
    const body = await response.json();

    assert.equal(response.status, 403);
    assert.equal(body.error, 'origin_not_allowed');
    assert.equal(response.headers.get('access-control-allow-origin'), null);
    assert.equal(response.headers.get('vary'), 'Origin');
  } finally {
    restoreEnv();
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
