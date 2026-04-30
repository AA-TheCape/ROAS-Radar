import type { Pool } from 'pg';

import { buildRawPayloadStorageMetadata } from '../src/shared/raw-payload-storage.js';

const INTEGRATION_RESET_LOCK_KEY = 4_240_042;

export function buildRawPayloadFixture(rawPayload: unknown, payloadExternalId?: string | null) {
  const metadata = buildRawPayloadStorageMetadata(rawPayload);

  return {
    rawPayload,
    rawPayloadJson: metadata.rawPayloadJson,
    payloadSizeBytes: metadata.payloadSizeBytes,
    payloadHash: metadata.payloadHash,
    payloadExternalId: payloadExternalId ?? null
  };
}

export async function resetIntegrationTables(pool: Pool, tables: string[]): Promise<void> {
  if (tables.length === 0) {
    return;
  }

  const client = await pool.connect();

  try {
    await client.query('SELECT pg_advisory_lock($1)', [INTEGRATION_RESET_LOCK_KEY]);
    await client.query(`
      TRUNCATE TABLE
        ${tables.join(',\n        ')}
      RESTART IDENTITY CASCADE
    `);
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [INTEGRATION_RESET_LOCK_KEY]).catch(() => undefined);
    client.release();
  }
}
