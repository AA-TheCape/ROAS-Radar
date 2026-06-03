import assert from 'node:assert/strict';
import test from 'node:test';

import type { PoolClient } from 'pg';

import {
  AttributionLookupPairError,
  resolveActiveAttributionLookupPair
} from '../src/modules/attribution/attribution-lookups.js';

test('resolveActiveAttributionLookupPair returns active source and method IDs for a valid pair', async () => {
  const queries: Array<{ sql: string; params: unknown[] }> = [];
  const client = {
    query: async (sql: string, params: unknown[]) => {
      queries.push({ sql, params });

      return {
        rows: [
          {
            attribution_source_id: 2,
            matching_method_id: 2
          }
        ]
      };
    }
  } as unknown as PoolClient;

  const pair = await resolveActiveAttributionLookupPair(client, {
    attributionSourceCode: 'checkout_token',
    matchingMethodCode: 'matched_by_checkout_token'
  });

  assert.deepEqual(pair, {
    attributionSourceId: 2,
    matchingMethodId: 2
  });
  assert.deepEqual(queries[0].params, ['checkout_token', 'matched_by_checkout_token']);
  assert.match(queries[0].sql, /methods\.attribution_source_id = sources\.id/);
  assert.match(queries[0].sql, /sources\.is_active = true/);
  assert.match(queries[0].sql, /methods\.is_active = true/);
});

test('resolveActiveAttributionLookupPair rejects missing, inactive, or mismatched pairs deterministically', async () => {
  const client = {
    query: async () => ({ rows: [] })
  } as unknown as PoolClient;

  await assert.rejects(
    () =>
      resolveActiveAttributionLookupPair(client, {
        attributionSourceCode: 'checkout_token',
        matchingMethodCode: 'matched_by_landing_session'
      }),
    (error: unknown) => {
      assert.equal(error instanceof AttributionLookupPairError, true);
      assert.equal((error as AttributionLookupPairError).code, 'attribution_lookup_pair_not_found');
      assert.match((error as Error).message, /checkout_token/);
      assert.match((error as Error).message, /matched_by_landing_session/);
      return true;
    }
  );
});
