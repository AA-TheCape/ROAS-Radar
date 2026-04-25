"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const promises_1 = require("node:fs/promises");
const node_path_1 = __importDefault(require("node:path"));
const node_url_1 = require("node:url");
const pool_js_1 = require("./pool.js");
const __filename = (0, node_url_1.fileURLToPath)(import.meta.url);
const __dirname = node_path_1.default.dirname(__filename);
const migrationsDir = node_path_1.default.resolve(__dirname, '../../db/migrations');
async function migrate() {
    const client = await pool_js_1.pool.connect();
    try {
        await client.query('BEGIN');
        await client.query('SELECT pg_advisory_xact_lock($1)', [7_204_202_6]);
        await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `);
        const files = (await (0, promises_1.readdir)(migrationsDir))
            .filter((file) => file.endsWith('.sql'))
            .sort();
        for (const file of files) {
            const existing = await client.query('SELECT filename FROM schema_migrations WHERE filename = $1', [file]);
            if (existing.rowCount) {
                continue;
            }
            const sql = await (0, promises_1.readFile)(node_path_1.default.join(migrationsDir, file), 'utf8');
            await client.query(sql);
            await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
            process.stdout.write(`Applied migration ${file}\n`);
        }
        await client.query('COMMIT');
    }
    catch (error) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw error;
    }
    finally {
        client.release();
        await pool_js_1.pool.end();
    }
}
migrate().catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    process.exit(1);
});
