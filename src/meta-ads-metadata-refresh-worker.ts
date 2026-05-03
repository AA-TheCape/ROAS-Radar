import { pool } from './db/pool.js';
import { refreshActiveMetaAdsMetadataConnections } from './modules/meta-ads/index.js';

async function run(): Promise<void> {
  const result = await refreshActiveMetaAdsMetadataConnections({
    workerId: process.env.K_SERVICE ?? 'meta-ads-metadata-refresh-worker'
  });

  process.stdout.write(`${JSON.stringify(result)}\n`);
  await pool.end();
}

run().catch(async (error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  await pool.end().catch(() => undefined);
  process.exit(1);
});
