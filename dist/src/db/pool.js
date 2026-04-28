"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.pool = void 0;
exports.query = query;
exports.withTransaction = withTransaction;
exports.checkDatabaseHealth = checkDatabaseHealth;
const pg_1 = require("pg");
const env_js_1 = require("../config/env.js");
const index_js_1 = require("../observability/index.js");
exports.pool = new pg_1.Pool({
    connectionString: env_js_1.env.DATABASE_URL,
    max: env_js_1.env.DATABASE_POOL_MAX,
    min: env_js_1.env.DATABASE_POOL_MIN,
    idleTimeoutMillis: env_js_1.env.DATABASE_IDLE_TIMEOUT_MS,
    connectionTimeoutMillis: env_js_1.env.DATABASE_CONNECTION_TIMEOUT_MS,
    statement_timeout: env_js_1.env.DATABASE_STATEMENT_TIMEOUT_MS,
    query_timeout: env_js_1.env.DATABASE_QUERY_TIMEOUT_MS,
    maxUses: env_js_1.env.DATABASE_MAX_USES,
    keepAlive: true,
    ssl: env_js_1.env.DATABASE_SSL ? { rejectUnauthorized: false } : undefined
});
exports.pool.on('error', (error) => {
    (0, index_js_1.logError)('database_pool_error', error, {
        service: process.env.K_SERVICE ?? 'roas-radar-api'
    });
});
async function query(text, params) {
    return exports.pool.query(text, params);
}
async function withTransaction(callback) {
    const client = await exports.pool.connect();
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
async function checkDatabaseHealth() {
    const startedAt = Date.now();
    await exports.pool.query('SELECT 1');
    return {
        ok: true,
        latencyMs: Date.now() - startedAt
    };
}
