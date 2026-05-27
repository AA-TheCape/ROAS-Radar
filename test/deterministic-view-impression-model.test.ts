import assert from 'node:assert/strict';
import test from 'node:test';

import type { PoolClient } from 'pg';

import {
  isDeterministicViewImpressionAttributionEnabled,
  persistDeterministicViewImpressionModelOutputs
} from '../src/modules/attribution/deterministic-view-impression-model.js';

function buildFakeClient(rowCount: number): {
  client: PoolClient;
  queries: Array<{ text: string; params?: unknown[] }>;
} {
  const queries: Array<{ text: string; params?: unknown[] }> = [];
  const client = {
    query: async (text: string, params?: unknown[]) => {
      queries.push({ text, params });
      return { rows: [], rowCount };
    }
  } as unknown as PoolClient;

  return { client, queries };
}

test('deterministic view/impression model is independently disabled by default', async () => {
  assert.equal(isDeterministicViewImpressionAttributionEnabled({}), false);

  const { client, queries } = buildFakeClient(1);
  const result = await persistDeterministicViewImpressionModelOutputs(client, {
    runId: '11111111-1111-4111-8111-111111111111',
    orderId: 'order-1',
    orderOccurredAtUtc: '2026-05-26T10:00:00.000Z',
    enabled: false
  });

  assert.deepEqual(result, {
    enabled: false,
    insertedRows: 0
  });
  assert.equal(queries.length, 0);
});

test('deterministic view/impression model writes separate outputs without click model mutation', async () => {
  assert.equal(
    isDeterministicViewImpressionAttributionEnabled({
      deterministicViewImpressionAttributionEnabled: true
    }),
    true
  );

  const { client, queries } = buildFakeClient(2);
  const result = await persistDeterministicViewImpressionModelOutputs(client, {
    runId: '11111111-1111-4111-8111-111111111111',
    orderId: 'order-1',
    orderOccurredAtUtc: '2026-05-26T10:00:00.000Z',
    enabled: true
  });

  assert.deepEqual(result, {
    enabled: true,
    insertedRows: 2
  });
  assert.equal(queries.length, 2);
  assert.match(queries[0].text, /DELETE FROM deterministic_model_outputs/);
  assert.match(queries[1].text, /INSERT INTO deterministic_model_outputs/);
  assert.match(queries[1].text, /deterministic_views/);
  assert.match(queries[1].text, /deterministic_impressions/);
  assert.doesNotMatch(queries.map((query) => query.text).join('\n'), /attribution_model_credits/);
  assert.doesNotMatch(queries.map((query) => query.text).join('\n'), /attribution_model_summaries/);
});
