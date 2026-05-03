import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const identityGraphWorkerModule = await import('../src/identity-graph-backfill-worker.js');
const googleAdsMetadataWorkerModule = await import('../src/google-ads-metadata-refresh-worker.js');
const metaAdsMetadataWorkerModule = await import('../src/meta-ads-metadata-refresh-worker.js');
const orderAttributionWorkerModule = await import('../src/order-attribution-materialization-worker.js');

const { resolveIdentityGraphBackfillExecution } = identityGraphWorkerModule;
const { resolveGoogleAdsMetadataRefreshExecution } = googleAdsMetadataWorkerModule;
const { resolveMetaAdsMetadataRefreshExecution } = metaAdsMetadataWorkerModule;
const { resolveOrderAttributionMaterializationExecution } = orderAttributionWorkerModule;

function withEnv<T>(overrides: Record<string, string | undefined>, callback: () => T): T {
  const previousEntries = Object.entries(overrides).map(([key]) => [key, process.env[key]] as const);

  for (const [key, value] of Object.entries(overrides)) {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }

  try {
    return callback();
  } finally {
    for (const [key, value] of previousEntries) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
}

test('resolveIdentityGraphBackfillExecution derives a recent backfill window and validates sources', () => {
  const execution = withEnv(
    {
      IDENTITY_GRAPH_BACKFILL_REQUESTED_BY: 'scheduler@roas-radar',
      IDENTITY_GRAPH_BACKFILL_WORKER_ID: 'identity-job-1',
      IDENTITY_GRAPH_BACKFILL_LOOKBACK_DAYS: '3',
      IDENTITY_GRAPH_BACKFILL_LAG_HOURS: '2',
      IDENTITY_GRAPH_BACKFILL_BATCH_SIZE: '500',
      IDENTITY_GRAPH_BACKFILL_MAX_BATCHES: '6',
      IDENTITY_GRAPH_BACKFILL_SOURCES: 'tracking_events,shopify_orders'
    },
    () => resolveIdentityGraphBackfillExecution(new Date('2026-04-26T12:00:00.000Z'))
  );

  assert.equal(execution.requestedBy, 'scheduler@roas-radar');
  assert.equal(execution.workerId, 'identity-job-1');
  assert.equal(execution.startAt.toISOString(), '2026-04-23T10:00:00.000Z');
  assert.equal(execution.endAt.toISOString(), '2026-04-26T10:00:00.000Z');
  assert.equal(execution.batchSize, 500);
  assert.equal(execution.maxBatches, 6);
  assert.deepEqual(execution.sources, ['tracking_events', 'shopify_orders']);
});

test('resolveOrderAttributionMaterializationExecution derives a lagged UTC date window', () => {
  const execution = withEnv(
    {
      ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY: 'scheduler@roas-radar',
      ORDER_ATTRIBUTION_MATERIALIZATION_WORKER_ID: 'materialization-job-1',
      ORDER_ATTRIBUTION_MATERIALIZATION_LOOKBACK_DAYS: '4',
      ORDER_ATTRIBUTION_MATERIALIZATION_LAG_DAYS: '2',
      ORDER_ATTRIBUTION_MATERIALIZATION_LIMIT: '250',
      ORDER_ATTRIBUTION_MATERIALIZATION_DRY_RUN: 'true',
      ORDER_ATTRIBUTION_MATERIALIZATION_ONLY_WEB_ORDERS: 'false',
      ORDER_ATTRIBUTION_MATERIALIZATION_SKIP_SHOPIFY_WRITEBACK: 'true'
    },
    () => resolveOrderAttributionMaterializationExecution(new Date('2026-04-26T12:34:56.000Z'))
  );

  assert.equal(execution.requestedBy, 'scheduler@roas-radar');
  assert.equal(execution.workerId, 'materialization-job-1');
  assert.equal(execution.windowStart.toISOString(), '2026-04-21T00:00:00.000Z');
  assert.equal(execution.windowEnd.toISOString(), '2026-04-24T23:59:59.999Z');
  assert.equal(execution.limit, 250);
  assert.equal(execution.dryRun, true);
  assert.equal(execution.onlyWebOrders, false);
  assert.equal(execution.writeToShopifyWhenAvailable, false);
});

test('resolveMetaAdsMetadataRefreshExecution uses explicit requested-by values for scheduler runs', () => {
  const execution = withEnv(
    {
      K_SERVICE: 'meta-ads-metadata-refresh-staging',
      META_ADS_METADATA_REFRESH_REQUESTED_BY: 'scheduler-meta-staging@roas-radar'
    },
    () => resolveMetaAdsMetadataRefreshExecution()
  );

  assert.equal(execution.requestedBy, 'scheduler-meta-staging@roas-radar');
  assert.equal(execution.workerId, 'meta-ads-metadata-refresh-staging');
});

test('resolveGoogleAdsMetadataRefreshExecution falls back to the Cloud Run scheduler marker', () => {
  const execution = withEnv(
    {
      K_SERVICE: 'google-ads-metadata-refresh-production',
      GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY: undefined
    },
    () => resolveGoogleAdsMetadataRefreshExecution()
  );

  assert.equal(execution.requestedBy, 'cloud-run-scheduler');
  assert.equal(execution.workerId, 'google-ads-metadata-refresh-production');
});
