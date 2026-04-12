import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const { __observabilityTestUtils } = await import('../src/observability/index.js');

test('parseCloudTraceContext maps Google trace headers into Cloud Logging format', () => {
  process.env.GOOGLE_CLOUD_PROJECT = 'roas-radar-prod';

  const trace = __observabilityTestUtils.parseCloudTraceContext(
    '105445aa7843bc8bf206b120001000/123;o=1'
  ) as Record<string, string | undefined>;

  assert.equal(trace.trace, 'projects/roas-radar-prod/traces/105445aa7843bc8bf206b120001000');
  assert.equal(trace.spanId, '123');
});

test('buildAttributionBacklogLog emits a structured backlog snapshot payload', () => {
  process.env.K_SERVICE = 'roas-radar-attribution-worker';

  const payload = JSON.parse(
    __observabilityTestUtils.buildAttributionBacklogLog({
      workerId: 'worker-1',
      pendingJobs: 42,
      oldestJobAgeSeconds: 180,
      staleProcessingJobs: 3
    })
  ) as Record<string, unknown>;

  assert.equal(payload.event, 'attribution_backlog_snapshot');
  assert.equal(payload.service, 'roas-radar-attribution-worker');
  assert.equal(payload.workerId, 'worker-1');
  assert.equal(payload.pendingJobs, 42);
  assert.equal(payload.oldestJobAgeSeconds, 180);
  assert.equal(payload.staleProcessingJobs, 3);
});
