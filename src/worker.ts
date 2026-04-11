import { pool } from './db/pool.js';
import { processPendingAttribution } from './modules/attribution/index.js';

async function run(): Promise<void> {
  const processed = await processPendingAttribution();
  process.stdout.write(`Processed ${processed} pending attribution record(s)\n`);
  await pool.end();
}

run().catch(async (error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  await pool.end().catch(() => undefined);
  process.exit(1);
});
