import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const enabledConfig = {
  enabled: true as const,
  ga4: {
    projectId: 'analytics-prod1',
    location: 'US',
    dataset: 'ga4_export',
    eventsTablePattern: 'events_*',
    intradayTablePattern: 'events_intraday_*',
    lookbackHours: 2,
    backfillHours: 2,
    eventsTableExpression: '`analytics-prod1.ga4_export.events_*`',
    intradayTableExpression: '`analytics-prod1.ga4_export.events_intraday_*`'
  },
  googleAdsTransfer: {
    projectId: 'analytics-prod1',
    location: 'US',
    dataset: 'google_ads_transfer',
    tablePattern: 'p_ads_*',
    lookbackDays: 14,
    tableExpression: '`analytics-prod1.google_ads_transfer.p_ads_*`',
    customerIds: []
  }
};

async function getModules() {
  const [poolModule, jobsModule, sessionModule, deadLetterModule] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/modules/attribution/ga4-ingestion-jobs.js'),
    import('../src/modules/attribution/ga4-session-attribution.js'),
    import('../src/modules/dead-letters/index.js')
  ]);

  return {
    pool: poolModule.pool,
    listHourlyRange: jobsModule.listHourlyRange,
    enqueueHours: jobsModule.enqueueHours,
    claimHourlyJobs: jobsModule.claimHourlyJobs,
    processGa4SessionAttributionHourlyJobs: jobsModule.processGa4SessionAttributionHourlyJobs,
    listGa4SessionAttributionRows: sessionModule.listGa4SessionAttributionRows,
    getGa4SessionAttributionWatermark: sessionModule.getGa4SessionAttributionWatermark,
    replayDeadLetters: deadLetterModule.replayDeadLetters,
    countPendingDeadLetters: deadLetterModule.countPendingDeadLetters
  };
}

async function resetGa4JobFixtures(): Promise<void> {
  const { pool } = await getModules();

  await pool.query(`
    TRUNCATE TABLE
      event_replay_run_items,
      event_replay_runs,
      event_dead_letters,
      ga4_bigquery_hourly_jobs,
      ga4_session_attribution,
      ga4_bigquery_ingestion_state
    RESTART IDENTITY CASCADE
  `);
}

type HourlyJobRow = {
  pipeline_name: string;
  hour_start: Date;
  status: string;
  attempts: number;
  requested_by: string | null;
  locked_by: string | null;
  last_error: string | null;
  dead_lettered_at: Date | null;
};

async function listHourlyJobs() {
  const { pool } = await getModules();
  const result = await pool.query<HourlyJobRow>(
    `
      SELECT
        pipeline_name,
        hour_start,
        status,
        attempts,
        requested_by,
        locked_by,
        last_error,
        dead_lettered_at
      FROM ga4_bigquery_hourly_jobs
      ORDER BY hour_start ASC
    `
  );

  return result.rows.map((row) => ({
    pipelineName: row.pipeline_name,
    hourStart: row.hour_start.toISOString(),
    status: row.status,
    attempts: row.attempts,
    requestedBy: row.requested_by,
    lockedBy: row.locked_by,
    lastError: row.last_error,
    deadLetteredAt: row.dead_lettered_at?.toISOString() ?? null
  }));
}

async function forceJobAvailable(hourStart: string): Promise<void> {
  const { pool } = await getModules();
  await pool.query(
    `
      UPDATE ga4_bigquery_hourly_jobs
      SET available_at = now() - interval '1 minute'
      WHERE pipeline_name = 'ga4_session_attribution'
        AND hour_start = $1::timestamptz
    `,
    [hourStart]
  );
}

async function listDeadLetters() {
  const { pool } = await getModules();
  const result = await pool.query<{
    event_type: string;
    source_table: string;
    source_record_id: string;
    source_queue_key: string | null;
    status: string;
    failure_count: number;
    last_error_message: string | null;
  }>(
    `
      SELECT
        event_type,
        source_table,
        source_record_id,
        source_queue_key,
        status,
        failure_count,
        last_error_message
      FROM event_dead_letters
      ORDER BY id ASC
    `
  );

  return result.rows;
}

function buildExecutorForRow(row: Record<string, unknown>) {
  return {
    async runQuery() {
      return [row];
    }
  };
}

test.beforeEach(async () => {
  await resetGa4JobFixtures();
});

test.after(async () => {
  await resetGa4JobFixtures();
  const { pool } = await getModules();
  await pool.end();
});

test('GA4 hourly job helpers enqueue explicit ranges once and claim them oldest first', async () => {
  const { listHourlyRange, enqueueHours, claimHourlyJobs } = await getModules();

  const hourStarts = listHourlyRange('2026-04-27T08:13:00.000Z', '2026-04-27T10:59:00.000Z');
  assert.deepEqual(hourStarts, [
    '2026-04-27T08:00:00.000Z',
    '2026-04-27T09:00:00.000Z',
    '2026-04-27T10:00:00.000Z'
  ]);

  const firstEnqueue = await enqueueHours({
    hourStarts: [...hourStarts, '2026-04-27T09:00:00.000Z'],
    requestedBy: 'integration-test'
  });
  assert.deepEqual(firstEnqueue, {
    hourStarts,
    enqueuedCount: 3
  });

  const claimed = await claimHourlyJobs({
    workerId: 'ga4-jobs-claim-worker',
    limit: 2,
    explicitHourStarts: ['2026-04-27T08:00:00.000Z', '2026-04-27T10:00:00.000Z']
  });
  assert.deepEqual(
    claimed.map((job) => ({
      pipelineName: job.pipelineName,
      hourStart: job.hourStart,
      attempts: job.attempts,
      requestedBy: job.requestedBy
    })),
    [
      {
        pipelineName: 'ga4_session_attribution',
        hourStart: '2026-04-27T08:00:00.000Z',
        attempts: 1,
        requestedBy: 'integration-test'
      },
      {
        pipelineName: 'ga4_session_attribution',
        hourStart: '2026-04-27T10:00:00.000Z',
        attempts: 1,
        requestedBy: 'integration-test'
      }
    ]
  );

  assert.deepEqual(await listHourlyJobs(), [
    {
      pipelineName: 'ga4_session_attribution',
      hourStart: '2026-04-27T08:00:00.000Z',
      status: 'processing',
      attempts: 1,
      requestedBy: 'integration-test',
      lockedBy: 'ga4-jobs-claim-worker',
      lastError: null,
      deadLetteredAt: null
    },
    {
      pipelineName: 'ga4_session_attribution',
      hourStart: '2026-04-27T09:00:00.000Z',
      status: 'pending',
      attempts: 0,
      requestedBy: 'integration-test',
      lockedBy: null,
      lastError: null,
      deadLetteredAt: null
    },
    {
      pipelineName: 'ga4_session_attribution',
      hourStart: '2026-04-27T10:00:00.000Z',
      status: 'processing',
      attempts: 1,
      requestedBy: 'integration-test',
      lockedBy: 'ga4-jobs-claim-worker',
      lastError: null,
      deadLetteredAt: null
    }
  ]);
});

test('GA4 hourly processing retries transient failures, then succeeds without duplicating persisted session rows', async () => {
  const {
    processGa4SessionAttributionHourlyJobs,
    listGa4SessionAttributionRows,
    getGa4SessionAttributionWatermark,
    countPendingDeadLetters
  } = await getModules();

  const targetHour = '2026-04-27T11:00:00.000Z';
  let calls = 0;
  const executor = {
    async runQuery() {
      calls += 1;
      if (calls === 1) {
        throw new Error('temporary BigQuery outage');
      }

      return [
        {
          ga4_session_key: 'pseudo-retry:1111',
          ga4_user_key: 'pseudo-retry',
          ga4_client_id: 'pseudo-retry',
          ga4_session_id: '1111',
          session_started_at: '2026-04-27T11:05:00.000Z',
          last_event_at: '2026-04-27T11:25:00.000Z',
          source: 'google',
          medium: 'cpc',
          campaign_id: '1111',
          campaign: 'Retry Campaign',
          content: 'Hero',
          term: 'boots',
          click_id_type: 'gclid',
          click_id_value: 'retry-click',
          account_id: '1234567890',
          account_name: 'Retry Account',
          channel_type: 'SEARCH',
          channel_subtype: 'SEARCH_STANDARD',
          campaign_metadata_source: 'google_ads_transfer',
          account_metadata_source: 'google_ads_transfer',
          channel_metadata_source: 'google_ads_transfer',
          source_export_hour: targetHour,
          source_dataset: 'ga4_export',
          source_table_type: 'events'
        }
      ];
    }
  };

  const firstRun = await processGa4SessionAttributionHourlyJobs({
    config: enabledConfig,
    executor,
    explicitHourStarts: [targetHour],
    workerId: 'ga4-retry-worker',
    requestedBy: 'integration-test',
    batchSize: 1,
    maxRetries: 3,
    initialBackoffSeconds: 7,
    maxBackoffSeconds: 30,
    now: new Date('2026-04-27T12:30:00.000Z')
  });

  assert.deepEqual(firstRun, {
    pipelineName: 'ga4_session_attribution',
    requestedBy: 'integration-test',
    workerId: 'ga4-retry-worker',
    seededHours: [targetHour],
    seededHourCount: 1,
    claimedHourCount: 1,
    claimedHours: [targetHour],
    succeededJobs: 0,
    retriedJobs: 1,
    deadLetteredJobs: 0
  });

  assert.deepEqual(await listHourlyJobs(), [
    {
      pipelineName: 'ga4_session_attribution',
      hourStart: targetHour,
      status: 'retry',
      attempts: 1,
      requestedBy: 'integration-test',
      lockedBy: null,
      lastError: 'temporary BigQuery outage',
      deadLetteredAt: null
    }
  ]);
  assert.equal(await countPendingDeadLetters(), 0);

  await forceJobAvailable(targetHour);

  const secondRun = await processGa4SessionAttributionHourlyJobs({
    config: enabledConfig,
    executor,
    explicitHourStarts: [targetHour],
    workerId: 'ga4-retry-worker',
    requestedBy: 'integration-test',
    batchSize: 1,
    maxRetries: 3,
    initialBackoffSeconds: 7,
    maxBackoffSeconds: 30,
    now: new Date('2026-04-27T12:35:00.000Z')
  });

  assert.deepEqual(secondRun, {
    pipelineName: 'ga4_session_attribution',
    requestedBy: 'integration-test',
    workerId: 'ga4-retry-worker',
    seededHours: [targetHour],
    seededHourCount: 1,
    claimedHourCount: 1,
    claimedHours: [targetHour],
    succeededJobs: 1,
    retriedJobs: 0,
    deadLetteredJobs: 0
  });

  assert.deepEqual(await listHourlyJobs(), [
    {
      pipelineName: 'ga4_session_attribution',
      hourStart: targetHour,
      status: 'completed',
      attempts: 2,
      requestedBy: 'integration-test',
      lockedBy: null,
      lastError: null,
      deadLetteredAt: null
    }
  ]);

  const persistedRows = await listGa4SessionAttributionRows((await getModules()).pool);
  assert.equal(persistedRows.length, 1);
  assert.deepEqual(persistedRows[0], {
    ga4SessionKey: 'pseudo-retry:1111',
    ga4UserKey: 'pseudo-retry',
    ga4ClientId: 'pseudo-retry',
    ga4SessionId: '1111',
    sessionStartedAt: '2026-04-27T11:05:00.000Z',
    lastEventAt: '2026-04-27T11:25:00.000Z',
    source: 'google',
    medium: 'cpc',
    campaignId: '1111',
    campaign: 'Retry Campaign',
    content: 'Hero',
    term: 'boots',
    clickIdType: 'gclid',
    clickIdValue: 'retry-click',
    accountId: '1234567890',
    accountName: 'Retry Account',
    channelType: 'SEARCH',
    channelSubtype: 'SEARCH_STANDARD',
    campaignMetadataSource: 'google_ads_transfer',
    accountMetadataSource: 'google_ads_transfer',
    channelMetadataSource: 'google_ads_transfer',
    sourceExportHour: targetHour,
    sourceDataset: 'ga4_export',
    sourceTableType: 'events'
  });

  assert.equal(await getGa4SessionAttributionWatermark((await getModules()).pool), targetHour);
  assert.equal(await countPendingDeadLetters(), 0);
});

test('GA4 hourly processing dead-letters exhausted failures and replay requeues the hour for another attempt', async () => {
  const { processGa4SessionAttributionHourlyJobs, replayDeadLetters, countPendingDeadLetters } = await getModules();

  const targetHour = '2026-04-27T12:00:00.000Z';
  const failingExecutor = {
    async runQuery() {
      throw new Error('permanent BigQuery failure');
    }
  };

  const deadLetterRun = await processGa4SessionAttributionHourlyJobs({
    config: enabledConfig,
    executor: failingExecutor,
    explicitHourStarts: [targetHour],
    workerId: 'ga4-dead-letter-worker',
    requestedBy: 'integration-test',
    batchSize: 1,
    maxRetries: 1,
    initialBackoffSeconds: 5,
    maxBackoffSeconds: 30,
    now: new Date('2026-04-27T13:10:00.000Z')
  });

  assert.deepEqual(deadLetterRun, {
    pipelineName: 'ga4_session_attribution',
    requestedBy: 'integration-test',
    workerId: 'ga4-dead-letter-worker',
    seededHours: [targetHour],
    seededHourCount: 1,
    claimedHourCount: 1,
    claimedHours: [targetHour],
    succeededJobs: 0,
    retriedJobs: 0,
    deadLetteredJobs: 1
  });

  const deadLetteredJobs = await listHourlyJobs();
  assert.equal(deadLetteredJobs.length, 1);
  assert.equal(deadLetteredJobs[0]?.pipelineName, 'ga4_session_attribution');
  assert.equal(deadLetteredJobs[0]?.hourStart, targetHour);
  assert.equal(deadLetteredJobs[0]?.status, 'dead_lettered');
  assert.equal(deadLetteredJobs[0]?.attempts, 1);
  assert.equal(deadLetteredJobs[0]?.requestedBy, 'integration-test');
  assert.equal(deadLetteredJobs[0]?.lockedBy, null);
  assert.equal(deadLetteredJobs[0]?.lastError, 'permanent BigQuery failure');
  assert.ok(deadLetteredJobs[0]?.deadLetteredAt);

  const deadLetters = await listDeadLetters();
  assert.equal(deadLetters.length, 1);
  assert.deepEqual(deadLetters[0], {
    event_type: 'ga4_session_attribution_hour_failed',
    source_table: 'ga4_bigquery_hourly_jobs',
    source_record_id: targetHour,
    source_queue_key: 'ga4_session_attribution',
    status: 'pending_replay',
    failure_count: 1,
    last_error_message: 'permanent BigQuery failure'
  });
  assert.equal(await countPendingDeadLetters(), 1);

  const replay = await replayDeadLetters({
    requestedBy: 'integration-test',
    eventType: 'ga4_session_attribution_hour_failed',
    sourceTable: 'ga4_bigquery_hourly_jobs',
    limit: 25,
    dryRun: false
  });
  assert.equal(replay.candidateCount, 1);
  assert.equal(replay.replayedCount, 1);
  assert.equal(replay.skippedCount, 0);
  assert.equal(replay.failedCount, 0);
  assert.equal(replay.dryRunCount, 0);

  assert.deepEqual(await listHourlyJobs(), [
    {
      pipelineName: 'ga4_session_attribution',
      hourStart: targetHour,
      status: 'pending',
      attempts: 1,
      requestedBy: 'integration-test',
      lockedBy: null,
      lastError: null,
      deadLetteredAt: null
    }
  ]);

  const replayedDeadLetters = await listDeadLetters();
  assert.equal(replayedDeadLetters[0]?.status, 'replayed');
  assert.equal(await countPendingDeadLetters(), 0);

  const successfulExecutor = buildExecutorForRow({
    ga4_session_key: 'pseudo-replayed:1212',
    ga4_user_key: 'pseudo-replayed',
    ga4_client_id: 'pseudo-replayed',
    ga4_session_id: '1212',
    session_started_at: '2026-04-27T12:10:00.000Z',
    last_event_at: '2026-04-27T12:32:00.000Z',
    source: 'email',
    medium: 'newsletter',
    campaign_id: null,
    campaign: 'Replay Campaign',
    content: null,
    term: null,
    click_id_type: null,
    click_id_value: null,
    account_id: null,
    account_name: null,
    channel_type: null,
    channel_subtype: null,
    campaign_metadata_source: 'ga4_raw',
    account_metadata_source: 'unresolved',
    channel_metadata_source: 'unresolved',
    source_export_hour: targetHour,
    source_dataset: 'ga4_export',
    source_table_type: 'events'
  });

  const completedRun = await processGa4SessionAttributionHourlyJobs({
    config: enabledConfig,
    executor: successfulExecutor,
    explicitHourStarts: [targetHour],
    workerId: 'ga4-dead-letter-worker',
    requestedBy: 'integration-test',
    batchSize: 1,
    maxRetries: 2,
    initialBackoffSeconds: 5,
    maxBackoffSeconds: 30,
    now: new Date('2026-04-27T13:20:00.000Z')
  });

  assert.equal(completedRun.succeededJobs, 1);
  assert.equal(completedRun.deadLetteredJobs, 0);
  assert.deepEqual((await listHourlyJobs()).map((job) => job.status), ['completed']);
});
