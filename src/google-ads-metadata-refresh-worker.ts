import { pathToFileURL } from 'node:url';

import { pool } from './db/pool.js';
import { refreshActiveGoogleAdsMetadataConnections } from './modules/google-ads/index.js';

export function resolveGoogleAdsMetadataRefreshExecution(): {
  requestedBy: string;
  workerId: string;
} {
  return {
    requestedBy:
      process.env.GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY?.trim() || 'cloud-run-scheduler',
    workerId: process.env.K_SERVICE ?? 'google-ads-metadata-refresh-worker'
  };
}

async function run(): Promise<void> {
  const execution = resolveGoogleAdsMetadataRefreshExecution();
  const result = await refreshActiveGoogleAdsMetadataConnections(execution);

  process.stdout.write(`${JSON.stringify(result)}\n`);
  await pool.end();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  run().catch(async (error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool.end().catch(() => undefined);
    process.exit(1);
  });
}
