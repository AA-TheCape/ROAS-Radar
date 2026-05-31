import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
const originalKService = process.env.K_SERVICE;
process.env.K_SERVICE = 'roas-radar-observability-test';

const {
  emitCampaignMetadataFreshnessSnapshotLog,
  emitCampaignMetadataResolutionCoverageLog,
  emitCampaignMetadataSyncJobLifecycleLog,
  emitRecoveryRecordFailureLog,
  emitRecoveryRunChunkLog,
  emitRecoveryRunLifecycleLog
} = await import('../src/observability/index.js');

const recoveryRun = {
  id: '123e4567-e89b-12d3-a456-426614174000',
  jobType: 'shopify_attribution_recovery',
  status: 'partial_failure',
  mode: 'automatic',
  initiatedBy: 'scheduler',
  dryRun: false,
  timeRangeStart: '2026-05-01T00:00:00.000Z',
  timeRangeEnd: '2026-05-02T00:00:00.000Z',
  idempotencyKey: 'recovery:key',
  concurrencyKey: 'range:key',
  scopeKey: 'shopify-attribution-hints',
  resumeFromRunId: null,
  rerunOfRunId: null,
  inputParameters: {},
  checkpoint: { offset: 10 },
  recordsDiscovered: 12,
  recordsClaimed: 12,
  recordsProcessed: 10,
  recordsSucceeded: 7,
  recordsFailed: 2,
  recordsSkipped: 1,
  recordsRetried: 1,
  sideEffectsAttempted: 7,
  sideEffectsSucceeded: 7,
  sideEffectsSuppressed: 1,
  claimedBy: 'worker-1',
  queuedAt: '2026-05-01T00:00:00.000Z',
  startedAt: '2026-05-01T00:00:05.000Z',
  completedAt: '2026-05-01T00:02:05.000Z',
  lastHeartbeatAt: '2026-05-01T00:02:05.000Z',
  errorCode: null,
  errorMessage: null
} as const;

test.after(() => {
  if (originalKService === undefined) {
    Reflect.deleteProperty(process.env, 'K_SERVICE');
    return;
  }

  process.env.K_SERVICE = originalKService;
});

async function captureStructuredLogs<T>(callback: () => T | Promise<T>): Promise<{
  entries: Array<Record<string, unknown>>;
  result: T;
}> {
  const stdoutChunks: string[] = [];
  const stderrChunks: string[] = [];
  const originalStdoutWrite = process.stdout.write.bind(process.stdout);
  const originalStderrWrite = process.stderr.write.bind(process.stderr);

  process.stdout.write = ((chunk: string | Uint8Array) => {
    stdoutChunks.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
    return true;
  }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: string | Uint8Array) => {
    stderrChunks.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
    return true;
  }) as typeof process.stderr.write;

  try {
    const result = await callback();
    const entries = [...stdoutChunks, ...stderrChunks]
      .join('')
      .trim()
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.startsWith('{') && line.endsWith('}'))
      .map((line) => JSON.parse(line) as Record<string, unknown>);

    return { entries, result };
  } finally {
    process.stdout.write = originalStdoutWrite as typeof process.stdout.write;
    process.stderr.write = originalStderrWrite as typeof process.stderr.write;
  }
}

test('campaign metadata resolution coverage logs include dashboard rates and trim unresolved samples', async () => {
  const { entries } = await captureStructuredLogs(() =>
    emitCampaignMetadataResolutionCoverageLog({
      resolutionScope: 'campaign_group',
      platform: 'google_ads',
      entityType: 'campaign',
      requestedCount: 4,
      matchedCount: 4,
      resolvedCount: 2,
      fallbackCount: 1,
      unresolvedCount: 1,
      unresolvedEntityIds: ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11'],
      startDate: '2026-04-01',
      endDate: '2026-04-02',
      source: 'google'
    })
  );

  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0], {
    severity: 'INFO',
    event: 'campaign_metadata_resolution_coverage',
    message: 'campaign_metadata_resolution_coverage',
    timestamp: entries[0]?.timestamp,
    service: 'roas-radar-observability-test',
    resolutionScope: 'campaign_group',
    platform: 'google_ads',
    entityType: 'campaign',
    requestedCount: 4,
    matchedCount: 4,
    resolvedCount: 2,
    fallbackCount: 1,
    unresolvedCount: 1,
    resolvedRate: 0.5,
    fallbackRate: 0.25,
    unresolvedRate: 0.25,
    unresolvedEntityIds: ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
    startDate: '2026-04-01',
    endDate: '2026-04-02',
    source: 'google'
  });
});

test('campaign metadata freshness snapshot logs expose the fields used by freshness dashboards', async () => {
  const { entries } = await captureStructuredLogs(() =>
    emitCampaignMetadataFreshnessSnapshotLog({
      platform: 'meta_ads',
      entityType: 'adset',
      freshEntityCount: 19,
      staleEntityCount: 3,
      freshnessThresholdHours: 30,
      oldestLastSeenAt: '2026-04-08T10:00:00.000Z',
      newestLastSeenAt: '2026-04-10T09:00:00.000Z'
    })
  );

  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0], {
    severity: 'INFO',
    event: 'campaign_metadata_freshness_snapshot',
    message: 'campaign_metadata_freshness_snapshot',
    timestamp: entries[0]?.timestamp,
    service: 'roas-radar-observability-test',
    platform: 'meta_ads',
    entityType: 'adset',
    freshEntityCount: 19,
    staleEntityCount: 3,
    freshnessThresholdHours: 30,
    oldestLastSeenAt: '2026-04-08T10:00:00.000Z',
    newestLastSeenAt: '2026-04-10T09:00:00.000Z'
  });
});

test('campaign metadata sync lifecycle logs emit success payloads and alertable failure payloads', async () => {
  const startedAt = '2026-04-11T10:00:00.000Z';
  const completedAt = '2026-04-11T10:00:04.250Z';
  const error = Object.assign(new Error('quota exhausted'), { code: 'quota_exhausted' });

  const { entries } = await captureStructuredLogs(async () => {
    emitCampaignMetadataSyncJobLifecycleLog({
      stage: 'completed',
      platform: 'google_ads',
      workerId: 'google-ads-metadata-refresh-worker',
      jobId: '123',
      requestedBy: 'cloud-run-scheduler',
      startedAt,
      completedAt,
      durationMs: 4250,
      plannedInserts: 12,
      plannedUpdates: 4,
      campaignResolvedRate: 0.9,
      overallUnresolvedRate: 0.1,
      staleEntityCount: 2
    });

    emitCampaignMetadataSyncJobLifecycleLog({
      stage: 'failed',
      platform: 'meta_ads',
      workerId: 'meta-ads-metadata-refresh-worker',
      jobId: '456',
      requestedBy: 'scheduler-meta',
      startedAt,
      completedAt,
      error
    });
  });

  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0], {
    severity: 'INFO',
    event: 'campaign_metadata_sync_job_lifecycle',
    message: 'campaign_metadata_sync_job_lifecycle',
    timestamp: entries[0]?.timestamp,
    service: 'roas-radar-observability-test',
    stage: 'completed',
    platform: 'google_ads',
    workerId: 'google-ads-metadata-refresh-worker',
    jobId: '123',
    requestedBy: 'cloud-run-scheduler',
    startedAt,
    completedAt,
    durationMs: 4250,
    plannedInserts: 12,
    plannedUpdates: 4,
    campaignResolvedRate: 0.9,
    overallUnresolvedRate: 0.1,
    staleEntityCount: 2
  });

  assert.deepEqual(entries[1], {
    severity: 'ERROR',
    event: 'campaign_metadata_sync_job_lifecycle',
    message: 'campaign_metadata_sync_job_lifecycle',
    timestamp: entries[1]?.timestamp,
    service: 'roas-radar-observability-test',
    stage: 'failed',
    platform: 'meta_ads',
    workerId: 'meta-ads-metadata-refresh-worker',
    jobId: '456',
    requestedBy: 'scheduler-meta',
    startedAt,
    completedAt,
    durationMs: null,
    plannedInserts: null,
    plannedUpdates: null,
    campaignResolvedRate: null,
    overallUnresolvedRate: null,
    staleEntityCount: null,
    alertable: true,
    error: {
      name: 'Error',
      message: 'quota exhausted',
      stack: entries[1]?.error && typeof entries[1].error === 'object'
        ? (entries[1].error as { stack?: string | null }).stack ?? null
        : null
    }
  });
});

test('recovery telemetry logs include run counters, chunk duration, trace correlation, and actionable failure context', async () => {
  const { entries } = await captureStructuredLogs(() => {
    emitRecoveryRunLifecycleLog({
      stage: 'completed',
      run: recoveryRun,
      workerId: 'worker-1',
      pagesProcessed: 2,
      durationMs: 120000
    });
    emitRecoveryRunChunkLog({
      run: recoveryRun,
      workerId: 'worker-1',
      pageNumber: 2,
      recordsDiscovered: 5,
      recordsProcessed: 4,
      done: true,
      durationMs: 3000,
      checkpoint: { offset: 10 }
    });
    emitRecoveryRecordFailureLog({
      run: recoveryRun,
      workerId: 'worker-1',
      recordId: '99',
      recordType: 'shopify_order',
      recordKey: 'gid://shopify/Order/1',
      attemptNumber: 3,
      retryable: false,
      nextAttemptAt: null,
      error: {
        code: 'shopify_write_failed',
        message: 'Shopify write failed',
        details: { status: 429 }
      }
    });
  });

  assert.equal(entries.length, 3);
  assert.equal(entries[0]?.event, 'recovery_run_lifecycle');
  assert.equal(entries[0]?.runId, recoveryRun.id);
  assert.equal(entries[0]?.jobType, recoveryRun.jobType);
  assert.equal(entries[0]?.processedCount, 10);
  assert.equal(entries[0]?.updatedCount, 7);
  assert.equal(entries[0]?.skippedCount, 1);
  assert.equal(entries[0]?.failedCount, 2);
  assert.equal(entries[0]?.durationMs, 120000);
  assert.equal(entries[0]?.recoveryTraceId, '123e4567e89b12d3a456426614174000');

  assert.equal(entries[1]?.event, 'recovery_run_chunk_processed');
  assert.equal(entries[1]?.pageNumber, 2);
  assert.equal(entries[1]?.recordsDiscovered, 5);
  assert.equal(entries[1]?.recordsProcessed, 4);
  assert.equal(entries[1]?.durationMs, 3000);

  assert.equal(entries[2]?.event, 'recovery_record_failure');
  assert.equal(entries[2]?.severity, 'ERROR');
  assert.equal(entries[2]?.errorCode, 'shopify_write_failed');
  assert.equal(entries[2]?.retryable, false);
  assert.equal(entries[2]?.alertable, true);
  assert.deepEqual(entries[2]?.errorDetails, { status: 429 });
  assert.match(
    (entries[2]?.actionContext as { inspectRunFilter?: string }).inspectRunFilter ?? '',
    /recovery_run_lifecycle/
  );
});
