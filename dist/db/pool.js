import { Pool } from 'pg';
import { env } from '../config/env.js';
import { logError } from '../observability/index.js';
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
    logError('database_pool_error', error, {
        service: process.env.K_SERVICE ?? 'roas-radar-api'
    });
});
export async function query(text, params) {
    return pool.query(text, params);
}
export async function withTransaction(callback) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const result = await callback(client);
        await client.query('COMMIT');
        return result;
    }
    catch (error) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw error;
    }
    finally {
        client.release();
    }
}
export async function checkDatabaseHealth() {
    const startedAt = Date.now();
    await pool.query('SELECT 1');
    return {
        ok: true,
        latencyMs: Date.now() - startedAt
    };
}
