import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const { __attributionTestUtils } = await import('../src/modules/attribution/index.js');

test('computeRetryDelaySeconds applies capped exponential backoff', () => {
  assert.equal(__attributionTestUtils.computeRetryDelaySeconds(1), 30);
  assert.equal(__attributionTestUtils.computeRetryDelaySeconds(2), 60);
  assert.equal(__attributionTestUtils.computeRetryDelaySeconds(3), 120);
  assert.equal(__attributionTestUtils.computeRetryDelaySeconds(10), 1800);
  assert.equal(__attributionTestUtils.computeRetryDelaySeconds(50), 1800);
});

test('buildQueueKey namespaces attribution jobs by order id', () => {
  assert.equal(__attributionTestUtils.buildQueueKey('12345'), 'order:12345');
});

test('buildProcessingMetricsLog emits structured worker metrics', () => {
  const payload = JSON.parse(
    __attributionTestUtils.buildProcessingMetricsLog({
      workerId: 'worker-1',
      modelVersion: 4,
      staleJobsEnqueued: 3,
      claimedJobs: 2,
      succeededJobs: 2,
      failedJobs: 0,
      durationMs: 125
    })
  ) as Record<string, unknown>;

  assert.equal(payload.event, 'attribution_queue_run');
  assert.equal(payload.workerId, 'worker-1');
  assert.equal(payload.modelVersion, 4);
  assert.equal(payload.staleJobsEnqueued, 3);
  assert.equal(payload.claimedJobs, 2);
  assert.equal(payload.succeededJobs, 2);
  assert.equal(payload.failedJobs, 0);
  assert.equal(payload.durationMs, 125);
});
