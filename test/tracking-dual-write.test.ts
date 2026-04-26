import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

let cachedModules:
  | {
      pool: typeof import('../src/db/pool.js').pool;
      closeServer: typeof import('../src/server.js').closeServer;
      createServer: typeof import('../src/server.js').createServer;
      originalPoolQuery: typeof import('../src/db/pool.js').pool.query;
      originalPoolConnect: typeof import('../src/db/pool.js').pool.connect;
    }
  | null = null;

const validTrackPayload = {
  eventType: 'page_view',
  occurredAt: '2026-04-23T12:00:00.000Z',
  sessionId: '123e4567-e89b-42d3-a456-426614174000',
  pageUrl: 'https://example.com/products/widget?utm_source=Google&utm_medium=CPC&utm_campaign=Spring&gclid=ABC123',
  referrerUrl: 'https://google.com/search?q=widget',
  shopifyCartToken: null,
  shopifyCheckoutToken: null,
  clientEventId: '223e4567-e89b-42d3-a456-426614174000',
  consentState: 'denied',
  context: {
    userAgent: 'Mozilla/5.0 Test Browser',
    screen: '1440x900',
    language: 'en-US'
  }
};

const rawPayloadWithExtraFields = {
  ...validTrackPayload,
  shopifyCartToken: '  cart-token-with-whitespace  ',
  shopifyCheckoutToken: '  checkout-token-with-whitespace  ',
  clientEventId: '  browser-event-with-whitespace  ',
  context: {
    ...validTrackPayload.context,
    userAgent: '  Mozilla/5.0 Test Browser  ',
    nested: {
      viewport: {
        width: 1440,
        height: 900
      }
    }
  },
  pageMetadata: {
    abTests: ['hero-a', 'pricing-b'],
    flags: {
      subscribed: false
    }
  },
  optionalFields: {
    emptyString: '',
    explicitNull: null
  }
};

async function getModules() {
  if (cachedModules) {
    return cachedModules;
  }

  const poolModule = await import('../src/db/pool.js');
  const serverModule = await import('../src/server.js');

  cachedModules = {
    pool: poolModule.pool,
    closeServer: serverModule.closeServer,
    createServer: serverModule.createServer,
    originalPoolQuery: poolModule.pool.query.bind(poolModule.pool),
    originalPoolConnect: poolModule.pool.connect.bind(poolModule.pool)
  };

  return cachedModules;
}

async function postTrack(
  server: { address(): AddressInfo | null },
  payload: unknown
): Promise<{ response: Response; body: Record<string, unknown> }> {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}/track`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json'
    },
    body: JSON.stringify(payload)
  });

  return {
    response,
    body: (await response.json()) as Record<string, unknown>
  };
}

function buildClientQueryHandler(
  queries: Array<{ transaction: number; text: string; params?: unknown[] }>,
  transactionNumber: number,
  options: {
    failServerAttribution?: boolean;
    deduplicateServerAttribution?: boolean;
  } = {}
) {
  return async (text: string, params?: unknown[]) => {
    queries.push({ transaction: transactionNumber, text, params });

    if (text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK') {
      return { rows: [] };
    }

    if (text.includes('INSERT INTO app_settings')) {
      return { rows: [] };
    }

    if (text.includes('SELECT reporting_timezone')) {
      return {
        rowCount: 1,
        rows: [{ reporting_timezone: 'America/Los_Angeles', updated_at: new Date('2026-04-23T00:00:00.000Z') }]
      };
    }

    if (text.includes('SELECT pg_advisory_xact_lock')) {
      return { rows: [] };
    }

    if (text.includes('DELETE FROM daily_reporting_metrics')) {
      return { rows: [] };
    }

    if (text.includes('INSERT INTO daily_reporting_metrics')) {
      return { rows: [] };
    }

    if (text.includes('SELECT DISTINCT o.shopify_order_id')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('INSERT INTO tracking_sessions')) {
      return { rows: [] };
    }

    if (text.includes('INSERT INTO tracking_events')) {
      return { rows: [] };
    }

    if (text.includes('INSERT INTO session_attribution_identities')) {
      if (options.failServerAttribution) {
        throw new Error('server attribution write failed');
      }

      return { rows: [] };
    }

    if (text.includes('INSERT INTO session_attribution_touch_events')) {
      if (options.deduplicateServerAttribution) {
        throw { code: '23505' };
      }

      return {
        rowCount: 1,
        rows: [
          {
            id: 88,
            captured_at: new Date('2026-04-23T12:00:05.000Z'),
            roas_radar_session_id: validTrackPayload.sessionId
          }
        ]
      };
    }

    throw new Error(`Unexpected client.query call in transaction ${transactionNumber}: ${text}`);
  };
}

test.afterEach(async () => {
  const { pool, originalPoolConnect, originalPoolQuery } = await getModules();
  pool.query = originalPoolQuery as typeof pool.query;
  pool.connect = originalPoolConnect as typeof pool.connect;
});

test('tracking endpoint dual-writes browser events into the attribution touch store', async () => {
  const { pool, createServer, closeServer } = await getModules();
  const queries: Array<{ transaction: number; text: string; params?: unknown[] }> = [];
  let connectCalls = 0;

  pool.query = (async (text: string) => {
    if (text.includes('WHERE client_event_id = $1')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('ingestion_source = ANY($5::text[])')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('FROM tracking_events') && text.includes('WHERE ingestion_fingerprint = $1')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('FROM session_attribution_touch_events')) {
      return { rowCount: 0, rows: [] };
    }

    throw new Error(`Unexpected pool.query call: ${text}`);
  }) as typeof pool.query;

  pool.connect = (async () => {
    connectCalls += 1;

    return {
      query: buildClientQueryHandler(queries, connectCalls),
      release: () => undefined
    };
  }) as typeof pool.connect;

  const server = createServer();

  try {
    const { response, body } = await postTrack(server, validTrackPayload);

    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.match(String(body.eventId), /^[0-9a-f-]{36}$/i);
    assert.equal(body.sessionId, validTrackPayload.sessionId);
    assert.equal(body.deduplicated, false);
    assert.equal(connectCalls, 2);

    const attribution = body.attribution as Record<string, unknown>;
    assert.equal(attribution.ok, true);
    assert.equal(attribution.touchEventId, 88);
    assert.equal(attribution.deduplicated, false);

    const browserEventInsert = queries.find(
      (entry) => entry.transaction === 1 && entry.text.includes('INSERT INTO tracking_events')
    );
    assert.ok(browserEventInsert);
    assert.equal(browserEventInsert.params?.[20], 'denied');
    assert.equal(browserEventInsert.params?.[22], 'browser');
    assert.deepEqual(JSON.parse(String(browserEventInsert.params?.[23])), validTrackPayload);

    const touchInsert = queries.find(
      (entry) => entry.transaction === 2 && entry.text.includes('INSERT INTO session_attribution_touch_events')
    );
    assert.ok(touchInsert);
    assert.equal(touchInsert.params?.[1], 'page_view');
    assert.equal(touchInsert.params?.[6], 'google');
    assert.equal(touchInsert.params?.[7], 'cpc');
    assert.equal(touchInsert.params?.[11], 'ABC123');
    assert.equal(touchInsert.params?.[17], 'denied');
    assert.equal(touchInsert.params?.[18], 'server');
    assert.deepEqual(JSON.parse(String(touchInsert.params?.[20])), validTrackPayload);
  } finally {
    await closeServer(server);
  }
});

test('tracking endpoint persists the full raw browser payload before schema filtering or normalization', async () => {
  const { pool, createServer, closeServer } = await getModules();
  const queries: Array<{ transaction: number; text: string; params?: unknown[] }> = [];
  let connectCalls = 0;

  pool.query = (async (text: string) => {
    if (text.includes('WHERE client_event_id = $1')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('ingestion_source = ANY($5::text[])')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('FROM tracking_events') && text.includes('WHERE ingestion_fingerprint = $1')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('FROM session_attribution_touch_events')) {
      return { rowCount: 0, rows: [] };
    }

    throw new Error(`Unexpected pool.query call: ${text}`);
  }) as typeof pool.query;

  pool.connect = (async () => {
    connectCalls += 1;

    return {
      query: buildClientQueryHandler(queries, connectCalls),
      release: () => undefined
    };
  }) as typeof pool.connect;

  const server = createServer();

  try {
    const { response, body } = await postTrack(server, rawPayloadWithExtraFields);

    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.equal(connectCalls, 2);

    const browserEventInsert = queries.find(
      (entry) => entry.transaction === 1 && entry.text.includes('INSERT INTO tracking_events')
    );
    assert.ok(browserEventInsert);
    assert.equal(browserEventInsert.params?.[17], 'cart-token-with-whitespace');
    assert.equal(browserEventInsert.params?.[18], 'checkout-token-with-whitespace');
    assert.equal(browserEventInsert.params?.[19], 'browser-event-with-whitespace');
    assert.deepEqual(JSON.parse(String(browserEventInsert.params?.[23])), rawPayloadWithExtraFields);

    const touchInsert = queries.find(
      (entry) => entry.transaction === 2 && entry.text.includes('INSERT INTO session_attribution_touch_events')
    );
    assert.ok(touchInsert);
    assert.equal(touchInsert.params?.[23], 'cart-token-with-whitespace');
    assert.equal(touchInsert.params?.[24], 'checkout-token-with-whitespace');
    assert.deepEqual(JSON.parse(String(touchInsert.params?.[20])), rawPayloadWithExtraFields);
  } finally {
    await closeServer(server);
  }
});

test('tracking endpoint keeps the browser event when the derived server attribution leg fails', async () => {
  const { pool, createServer, closeServer } = await getModules();
  let connectCalls = 0;

  pool.query = (async (text: string) => {
    if (
      text.includes('WHERE client_event_id = $1') ||
      text.includes('ingestion_source = ANY($5::text[])') ||
      (text.includes('FROM tracking_events') && text.includes('WHERE ingestion_fingerprint = $1'))
    ) {
      return { rowCount: 0, rows: [] };
    }

    throw new Error(`Unexpected pool.query call: ${text}`);
  }) as typeof pool.query;

  pool.connect = (async () => {
    connectCalls += 1;

    return {
      query: buildClientQueryHandler([], connectCalls, { failServerAttribution: connectCalls === 2 }),
      release: () => undefined
    };
  }) as typeof pool.connect;

  const server = createServer();

  try {
    const { response, body } = await postTrack(server, validTrackPayload);

    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.match(String(body.eventId), /^[0-9a-f-]{36}$/i);

    const attribution = body.attribution as Record<string, unknown>;
    assert.equal(attribution.ok, false);
    assert.equal(attribution.touchEventId, null);
    assert.equal(attribution.errorCode, 'server_attribution_emit_failed');
    assert.equal(connectCalls, 2);
  } finally {
    await closeServer(server);
  }
});

test('tracking endpoint deduplicates the backend-derived attribution leg against an existing mirrored touch', async () => {
  const { pool, createServer, closeServer } = await getModules();
  let connectCalls = 0;

  pool.query = (async (text: string) => {
    if (text.includes('WHERE client_event_id = $1')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('ingestion_source = ANY($5::text[])')) {
      return { rowCount: 0, rows: [] };
    }

    if (text.includes('FROM tracking_events') && text.includes('WHERE ingestion_fingerprint = $1')) {
      return {
        rowCount: 1,
        rows: [
          {
            id: 'existing-browser-event',
            occurred_at: new Date('2026-04-23T12:00:00.000Z'),
            ingested_at: new Date('2026-04-23T12:00:01.000Z'),
            session_id: validTrackPayload.sessionId
          }
        ]
      };
    }

    if (text.includes('FROM session_attribution_touch_events')) {
      return {
        rowCount: 1,
        rows: [
          {
            id: 77,
            captured_at: new Date('2026-04-23T12:00:05.000Z'),
            roas_radar_session_id: validTrackPayload.sessionId
          }
        ]
      };
    }

    throw new Error(`Unexpected pool.query call: ${text}`);
  }) as typeof pool.query;

  pool.connect = (async () => {
    connectCalls += 1;

    return {
      query: buildClientQueryHandler([], connectCalls, { deduplicateServerAttribution: connectCalls === 1 }),
      release: () => undefined
    };
  }) as typeof pool.connect;

  const server = createServer();

  try {
    const { response, body } = await postTrack(server, validTrackPayload);

    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.equal(body.eventId, 'existing-browser-event');
    assert.equal(body.deduplicated, true);

    const attribution = body.attribution as Record<string, unknown>;
    assert.equal(attribution.ok, true);
    assert.equal(attribution.touchEventId, 77);
    assert.equal(attribution.deduplicated, true);
    assert.equal(connectCalls, 1);
  } finally {
    await closeServer(server);
  }
});
