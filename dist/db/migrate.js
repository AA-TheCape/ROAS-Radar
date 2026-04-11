import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool } from './pool.js';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const migrationsDir = path.resolve(__dirname, '../../db/migrations');
async function migrate() {
    const client = await pool.connect();
    try {
        await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `);
        const files = (await readdir(migrationsDir))
            .filter((file) => file.endsWith('.sql'))
            .sort();
        for (const file of files) {
            const existing = await client.query('SELECT filename FROM schema_migrations WHERE filename = $1', [file]);
            if (existing.rowCount) {
                continue;
            }
            const sql = await readFile(path.join(migrationsDir, file), 'utf8');
            await client.query(sql);
            await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
            process.stdout.write(`Applied migration ${file}\n`);
        }
    }
    catch (error) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw error;
    }
    finally {
        client.release();
        await pool.end();
    }
}
migrate().catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    process.exit(1);
});
