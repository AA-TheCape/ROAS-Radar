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
const originalPoolConnect = pool.connect.bind(pool);

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
      connection: 'close',
      ...(input.headers ?? {})
    },
    body: input.body === undefined ? undefined : JSON.stringify(input.body)
  });

  return {
    response,
    body: (await response.json()) as Record<string, unknown>
  };
}

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

test.afterEach(() => {
  pool.query = originalPoolQuery as typeof pool.query;
  pool.connect = originalPoolConnect as typeof pool.connect;
});

test('meta attribution evidence admin route rejects unauthorized requests', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/meta-ads/attribution-evidence', {
      method: 'POST',
      body: {
        shopifyOrderId: '1001',
        metaSignalId: 'meta-signal-1',
        reportedAtUtc: '2026-04-10T12:00:00.000Z',
        metaAttributionReason: 'purchase',
        campaignId: 'cmp_1',
        adAccountId: 'act_123'
      }
    });

    assert.equal(response.status, 401);
    assert.deepEqual(body, {
      error: 'unauthorized',
      message: 'Authentication required'
    });
  } finally {
    await closeServer(server);
  }
});

test('meta attribution evidence route rejects invalid payloads and emits an audit log entry', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { entries, result } = await captureStructuredLogs(async () =>
      requestJson(server, '/api/admin/meta-ads/attribution-evidence', {
        method: 'POST',
        headers: {
          authorization: 'Bearer test-reporting-token'
        },
        body: {
          shopifyOrderId: '1001',
          metaSignalId: 'meta-signal-bad',
          reportedAtUtc: '2026-04-10T12:00:00',
          metaAttributionReason: 'purchase',
          campaignId: 'cmp_1',
          adAccountId: 'act_123'
        }
      })
    );

    assert.equal(result.response.status, 400);
    assert.equal(result.body.error, 'invalid_request');
    assert.equal(
      result.body.message,
      'reportedAtUtc must be an ISO-8601 timestamp with an explicit UTC offset'
    );
    assert.equal(queryCalls, 1);

    const rejectionLog = entries.find((entry) => entry.event === 'meta_order_attribution_evidence_rejected');
    assert.ok(rejectionLog);
    assert.equal(rejectionLog?.code, 'invalid_request');
    assert.equal(rejectionLog?.shopifyOrderId, '1001');
    assert.equal(rejectionLog?.metaSignalId, 'meta-signal-bad');
  } finally {
    await closeServer(server);
  }
});

test('meta attribution evidence route persists normalized payloads with canonical eligibility flags', async () => {
  const capturedPoolQueries: Array<{ text: string; params?: unknown[] }> = [];
  const capturedClientQueries: Array<{ text: string; params?: unknown[] }> = [];
  const rawPayload = {
    order_reference: '1001',
    nested: {
      untouched: true
    }
  };

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedPoolQueries.push({ text, params });

    if (text.includes('FROM shopify_orders')) {
      return {
        rows: [
          {
            shopify_order_id: '1001',
            processed_at: new Date('2026-04-10T18:00:00.000Z'),
            created_at_shopify: new Date('2026-04-10T17:55:00.000Z'),
            ingested_at: new Date('2026-04-10T18:05:00.000Z')
          }
        ]
      };
    }

    throw new Error(`Unexpected pool.query call: ${text}`);
  }) as typeof pool.query;

  pool.connect = (async () => ({
    query: async (text: string | { text: string; values?: unknown[] }, params?: unknown[]) => {
      const normalizedText = typeof text === 'string' ? text : text.text;
      const normalizedParams = typeof text === 'string' ? params : (text.values ?? params);
      capturedClientQueries.push({ text: normalizedText, params: normalizedParams });

      if (normalizedText === 'BEGIN' || normalizedText === 'COMMIT' || normalizedText === 'ROLLBACK') {
        return { rows: [] };
      }

      if (normalizedText.includes('INSERT INTO meta_order_attribution_evidence')) {
        return {
          rows: [
            {
              id: '3d9f4ed4-0c41-4f32-96c7-73f149f6a0a8',
              eligibility_outcome: 'eligible_canonical',
              confidence_score: '0.72',
              order_occurred_at_utc: new Date('2026-04-10T18:00:00.000Z'),
              meta_touchpoint_occurred_at_utc: new Date('2026-04-09T16:30:00.000Z'),
              match_basis: 'fbc',
              currency_code: 'USD'
            }
          ]
        };
      }

      throw new Error(`Unexpected client.query call: ${normalizedText}`);
    },
    release: () => undefined
  })) as typeof pool.connect;

  const server = createServer();

  try {
    const { entries, result } = await captureStructuredLogs(async () =>
      requestJson(server, '/api/admin/meta-ads/attribution-evidence', {
        method: 'POST',
        headers: {
          authorization: 'Bearer test-reporting-token'
        },
        body: {
          organizationId: 77,
          shopifyOrderId: '1001',
          metaConnectionId: 14,
          rawRecordId: 22,
          syncJobId: 31,
          ingestionRunId: 9,
          metaSignalId: 'meta-signal-1',
          sourceKind: 'order_joinable',
          reportedAtUtc: '2026-04-10T19:00:00.000Z',
          sourceReceivedAt: '2026-04-10T19:02:00.000Z',
          metaTouchpointOccurredAtUtc: '2026-04-09T16:30:00.000Z',
          eventOrReportTimestampUtc: '2026-04-10T18:59:00.000Z',
          reportedConversionTimestampUtc: '2026-04-10T18:00:00.000Z',
          attributionWindowDays: 7,
          metaAttributionReason: 'purchase',
          campaignId: 'cmp_9',
          campaignName: 'Meta Campaign',
          adAccountId: ' act_123456 ',
          adId: 'ad_7',
          adSetId: 'adset_3',
          currencyCode: ' usd ',
          reportedConversionValue: '149.99',
          reportedEventName: 'Purchase',
          isViewThrough: false,
          isClickThrough: true,
          matchBasis: ' FBC ',
          observedMatchBases: ['fbclid', 'FBC'],
          confidenceScore: 0.72,
          rawPayload: rawPayload,
          sourceRecordIds: ['meta_raw_1', 22]
        }
      })
    );

    assert.equal(result.response.status, 201);
    assert.equal(result.body.ok, true);
    assert.equal(result.body.eligibilityOutcome, 'eligible_canonical');
    assert.equal(result.body.confidenceScore, 0.72);
    assert.deepEqual(result.body.normalized, {
      shopifyOrderId: '1001',
      metaSignalId: 'meta-signal-1',
      orderOccurredAtUtc: '2026-04-10T18:00:00.000Z',
      metaTouchpointOccurredAtUtc: '2026-04-09T16:30:00.000Z',
      matchBasis: 'fbc',
      currencyCode: 'USD'
    });

    assert.ok(capturedClientQueries.length >= 2);

    const successLog = entries.find((entry) => entry.event === 'meta_order_attribution_evidence_ingested');
    assert.ok(successLog);
    assert.equal(successLog?.eligibilityOutcome, 'eligible_canonical');
    assert.equal(successLog?.organizationId, 77);
    assert.equal(successLog?.metaSignalId, 'meta-signal-1');
  } finally {
    await closeServer(server);
  }
});
