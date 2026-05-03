import { pool } from './db/pool.js';
import { refreshActiveGoogleAdsMetadataConnections } from './modules/google-ads/index.js';
async function run() {
    const result = await refreshActiveGoogleAdsMetadataConnections({
        workerId: process.env.K_SERVICE ?? 'google-ads-metadata-refresh-worker'
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    await pool.end();
}
run().catch(async (error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool.end().catch(() => undefined);
    process.exit(1);
});
