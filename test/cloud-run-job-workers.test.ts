import assert from 'node:assert/strict';
import test from 'node:test';

const { resolveGa4IngestionExecution } = await import('../src/modules/attribution/ga4-ingestion-execution.js');

const ENV_KEYS = [
  'GA4_INGESTION_REQUESTED_BY',
  'GA4_INGESTION_WORKER_ID',
  'GA4_INGESTION_START_HOUR',
  'GA4_INGESTION_END_HOUR',
  'GA4_INGESTION_BATCH_SIZE',
  'GA4_INGESTION_MAX_RETRIES',
  'GA4_INGESTION_INITIAL_BACKOFF_SECONDS',
  'GA4_INGESTION_MAX_BACKOFF_SECONDS',
  'GA4_INGESTION_STALE_LOCK_MINUTES',
  'K_SERVICE',
  'K_JOB',
  'K_JOB_EXECUTION'
] as const;

const ORIGINAL_ENV = new Map(ENV_KEYS.map((key) => [key, process.env[key]]));

function resetEnv() {
  for (const key of ENV_KEYS) {
    const value = ORIGINAL_ENV.get(key);
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
}

test.beforeEach(() => {
  resetEnv();
});

test.after(() => {
  resetEnv();
});

test('resolveGa4IngestionExecution normalizes an inclusive explicit hour range', () => {
  process.env.GA4_INGESTION_REQUESTED_BY = 'cloud-run-scheduler-staging';
  process.env.K_JOB_EXECUTION = 'ga4-execution-1';
  process.env.GA4_INGESTION_START_HOUR = '2026-04-27T08:13:00.000Z';
  process.env.GA4_INGESTION_END_HOUR = '2026-04-27T10:59:59.999Z';

  const execution = resolveGa4IngestionExecution(new Date('2026-04-27T11:00:00.000Z'));

  assert.deepEqual(execution, {
    requestedBy: 'cloud-run-scheduler-staging',
    workerId: 'ga4-execution-1',
    explicitHourStarts: [
      '2026-04-27T08:00:00.000Z',
      '2026-04-27T09:00:00.000Z',
      '2026-04-27T10:00:00.000Z'
    ],
    batchSize: 24,
    maxRetries: 5,
    initialBackoffSeconds: 30,
    maxBackoffSeconds: 1800,
    staleLockMinutes: 30
  });
});

test('resolveGa4IngestionExecution honors batch, retry, and stale-lock env overrides', () => {
  process.env.K_SERVICE = 'roas-radar-ga4-session-attribution';
  process.env.GA4_INGESTION_WORKER_ID = 'ga4-worker-1';
  process.env.GA4_INGESTION_BATCH_SIZE = '6';
  process.env.GA4_INGESTION_MAX_RETRIES = '5';
  process.env.GA4_INGESTION_INITIAL_BACKOFF_SECONDS = '300';
  process.env.GA4_INGESTION_MAX_BACKOFF_SECONDS = '21600';
  process.env.GA4_INGESTION_STALE_LOCK_MINUTES = '45';

  const execution = resolveGa4IngestionExecution(new Date('2026-04-27T11:00:00.000Z'));

  assert.equal(execution.requestedBy, 'roas-radar-ga4-session-attribution');
  assert.equal(execution.workerId, 'ga4-worker-1');
  assert.equal(execution.explicitHourStarts, undefined);
  assert.equal(execution.batchSize, 6);
  assert.equal(execution.maxRetries, 5);
  assert.equal(execution.initialBackoffSeconds, 300);
  assert.equal(execution.maxBackoffSeconds, 21600);
  assert.equal(execution.staleLockMinutes, 45);
});

test('resolveGa4IngestionExecution rejects invalid positive integer overrides', () => {
  process.env.GA4_INGESTION_BATCH_SIZE = '0';

  assert.throws(
    () => resolveGa4IngestionExecution(new Date('2026-04-27T11:00:00.000Z')),
    /Invalid GA4_INGESTION_BATCH_SIZE value: 0/
  );
});
