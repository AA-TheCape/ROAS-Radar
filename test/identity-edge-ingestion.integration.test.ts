import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

import { resetE2EDatabase } from './e2e-harness.ts';

async function getIdentityModules() {
  const [{ withTransaction }, { ingestIdentityEdges }] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/modules/identity/index.js')
  ]);

  return {
    withTransaction,
    ingestIdentityEdges
  };
}

async function captureStructuredLogs<T>(callback: () => Promise<T>): Promise<{ entries: Array<Record<string, unknown>>; result: T }> {
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

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  const { pool } = await import('../src/db/pool.js');
  await pool.end();
});

test('identity edge ingestion is idempotent for repeated runs', async () => {
  const sessionId = '123e4567-e89b-42d3-a456-426614174000';
  const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

  const first = await withTransaction((client) =>
    ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-20T10:00:00.000Z',
      evidenceSource: 'tracking_event',
      sourceTable: 'tracking_events',
      sourceRecordId: 'event-1',
      idempotencyKey: 'identity-replay-1',
      sessionId,
      checkoutToken: 'co-1',
      cartToken: 'ca-1'
    })
  );
  const second = await withTransaction((client) =>
    ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-20T10:00:00.000Z',
      evidenceSource: 'tracking_event',
      sourceTable: 'tracking_events',
      sourceRecordId: 'event-1',
      idempotencyKey: 'identity-replay-1',
      sessionId,
      checkoutToken: 'co-1',
      cartToken: 'ca-1'
    })
  );

  assert.equal(first.outcome, 'linked');
  assert.equal(second.outcome, 'linked');
  assert.equal(second.deduplicated, true);

  const { pool } = await import('../src/db/pool.js');
  const countResult = await pool.query<{
    node_count: string;
    edge_count: string;
    run_count: string;
  }>(
    `
      SELECT
        (SELECT COUNT(*)::text FROM identity_nodes) AS node_count,
        (SELECT COUNT(*)::text FROM identity_edges) AS edge_count,
        (SELECT COUNT(*)::text FROM identity_edge_ingestion_runs) AS run_count
    `
  );

  assert.equal(countResult.rows[0]?.node_count, '3');
  assert.equal(countResult.rows[0]?.edge_count, '3');
  assert.equal(countResult.rows[0]?.run_count, '1');
});

test('identity edge ingestion preserves first_seen and last_seen across out-of-order events', async () => {
  const sessionId = '123e4567-e89b-42d3-a456-426614174001';
  const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

  await withTransaction((client) =>
    ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-24T12:00:00.000Z',
      evidenceSource: 'tracking_event',
      sourceTable: 'tracking_events',
      sourceRecordId: 'event-newer',
      idempotencyKey: 'identity-order-1',
      sessionId,
      checkoutToken: 'co-2'
    })
  );

  await withTransaction((client) =>
    ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-18T08:00:00.000Z',
      evidenceSource: 'tracking_event',
      sourceTable: 'tracking_events',
      sourceRecordId: 'event-older',
      idempotencyKey: 'identity-order-2',
      sessionId,
      cartToken: 'ca-2'
    })
  );

  const { pool } = await import('../src/db/pool.js');
  const nodeResult = await pool.query<{
    first_seen_at: Date;
    last_seen_at: Date;
  }>(
    `
      SELECT first_seen_at, last_seen_at
      FROM identity_nodes
      WHERE node_type = 'session_id'
        AND node_key = $1
    `,
    [sessionId]
  );
  const edgeResult = await pool.query<{
    first_observed_at: Date;
    last_observed_at: Date;
  }>(
    `
      SELECT e.first_observed_at, e.last_observed_at
      FROM identity_edges e
      INNER JOIN identity_nodes n ON n.id = e.node_id
      WHERE n.node_type = 'session_id'
        AND n.node_key = $1
        AND e.is_active = true
    `,
    [sessionId]
  );

  assert.equal(nodeResult.rows[0]?.first_seen_at.toISOString(), '2026-04-18T08:00:00.000Z');
  assert.equal(nodeResult.rows[0]?.last_seen_at.toISOString(), '2026-04-24T12:00:00.000Z');
  assert.equal(edgeResult.rows[0]?.first_observed_at.toISOString(), '2026-04-18T08:00:00.000Z');
  assert.equal(edgeResult.rows[0]?.last_observed_at.toISOString(), '2026-04-24T12:00:00.000Z');
});

test('identity edge ingestion emits structured processing metrics', async () => {
  const { withTransaction, ingestIdentityEdges } = await getIdentityModules();
  const { entries, result } = await captureStructuredLogs(() =>
    withTransaction((client) =>
      ingestIdentityEdges(client, {
        sourceTimestamp: '2026-04-22T14:00:00.000Z',
        evidenceSource: 'tracking_event',
        sourceTable: 'tracking_events',
        sourceRecordId: 'event-metrics',
        idempotencyKey: 'identity-metrics-1',
        sessionId: '123e4567-e89b-42d3-a456-426614174002',
        checkoutToken: 'co-3',
        cartToken: 'ca-3'
      })
    )
  );

  assert.equal(result.outcome, 'linked');

  const identityMetricsLog = entries.find((entry) => entry.event === 'identity_edge_ingestion_processed');
  assert.ok(identityMetricsLog);
  assert.equal(identityMetricsLog?.evidenceSource, 'tracking_event');
  assert.equal(identityMetricsLog?.outcome, 'linked');
  assert.equal(identityMetricsLog?.processedNodes, 3);
  assert.equal(identityMetricsLog?.attachedNodes, 3);
  assert.equal(identityMetricsLog?.rehomedNodes, 0);
  assert.equal(identityMetricsLog?.quarantinedNodes, 0);
});
