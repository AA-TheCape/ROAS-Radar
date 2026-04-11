import { Pool, type PoolClient, type QueryResult, type QueryResultRow } from 'pg';

import { env } from '../config/env.js';

export const pool = new Pool({
  connectionString: env.DATABASE_URL,
  max: env.DATABASE_POOL_MAX,
  min: env.DATABASE_POOL_MIN,
  idleTimeoutMillis: env.DATABASE_IDLE_TIMEOUT_MS,
  connectionTimeoutMillis: env.DATABASE_CONNECTION_TIMEOUT_MS,
  statement_timeout: env.DATABASE_STATEMENT_TIMEOUT_MS,
  query_timeout: env.DATABASE_QUERY_TIMEOUT_MS,
  maxUses: env.DATABASE_MAX_USES,
  keepAlive: true,
  ssl: env.DATABASE_SSL ? { rejectUnauthorized: false } : undefined
});

pool.on('error', (error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
});

export async function query<TResult extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[]
): Promise<QueryResult<TResult>> {
  return pool.query<TResult>(text, params);
}

export async function withTransaction<TResult>(callback: (client: PoolClient) => Promise<TResult>): Promise<TResult> {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

export async function checkDatabaseHealth(): Promise<{ ok: boolean; latencyMs: number }> {
  const startedAt = Date.now();
  await pool.query('SELECT 1');

  return {
    ok: true,
    latencyMs: Date.now() - startedAt
  };
}
