import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

const { runMetaAdsOrderValueSync } = await import('../src/modules/meta-ads/index.js');

async function captureStructuredLogs<T>(callback: () => Promise<T>): Promise<{
  entries: Array<Record<string, unknown>>;
  result: T;
}> {
  const entries: Array<Record<string, unknown>> = [];
  const originalStdoutWrite = process.stdout.write.bind(process.stdout);
  const originalStderrWrite = process.stderr.write.bind(process.stderr);

  const capture =
    (originalWrite: typeof process.stdout.write) =>
    ((chunk: string | Uint8Array, encoding?: BufferEncoding | ((error?: Error | null) => void), callback?: (error?: Error | null) => void) => {
      const text = typeof chunk === 'string' ? chunk : chunk.toString(typeof encoding === 'string' ? encoding : 'utf8');

      for (const line of text.split('\n')) {
        const trimmed = line.trim();

        if (!trimmed) {
          continue;
        }

        try {
          entries.push(JSON.parse(trimmed) as Record<string, unknown>);
        } catch {
          // Ignore non-JSON writes.
        }
      }

      return originalWrite(chunk, encoding as never, callback as never);
    }) as typeof process.stdout.write;

  process.stdout.write = capture(originalStdoutWrite);
  process.stderr.write = capture(originalStderrWrite);

  try {
    const result = await callback();
    return { entries, result };
  } finally {
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
  }
}

test('runMetaAdsOrderValueSync upserts current-window aggregates without duplicating rows across repeated hourly runs', async () => {
  const { entries, result: firstRun } = await captureStructuredLogs(() =>
    runMetaAdsOrderValueSync({
      now: new Date('2026-04-29T15:00:00.000Z'),
      triggerSource: 'test'
    })
  );

  assert.equal(firstRun.succeededConnections, 1);
  assert.equal(firstRun.recordsReceived, 3);
  assert.equal(firstRun.aggregateRowsUpserted, 3);

  const apiRequestLogs = entries.filter((entry) => entry.event === 'meta_ads_api_request_completed');
  assert.equal(apiRequestLogs.length, 2);

  const connectionCompletedLog = entries.find(
    (entry) => entry.event === 'meta_ads_order_value_sync_connection_completed'
  );
  assert.ok(connectionCompletedLog);
  assert.equal(connectionCompletedLog.rawRowsFetched, 4);
  assert.equal(connectionCompletedLog.normalizedRecordsReceived, 3);
  assert.equal(connectionCompletedLog.aggregateRowsUpserted, 3);
  assert.equal(connectionCompletedLog.apiRequestCount, 2);
  assert.equal(connectionCompletedLog.apiRequestErrorCount, 0);
  assert.equal(connectionCompletedLog.zeroRowsPulled, false);

  const overallCompletedLog = entries.find((entry) => entry.event === 'meta_ads_order_value_sync_completed');
  assert.ok(overallCompletedLog);
  assert.equal(overallCompletedLog.apiRequestCount, 2);
  assert.equal(overallCompletedLog.anomalyCount, 0);
});

test('runMetaAdsOrderValueSync emits a zero-row anomaly when Meta returns no campaign-day order-value data', async () => {
  const { entries, result } = await captureStructuredLogs(() =>
    runMetaAdsOrderValueSync({
      now: new Date('2026-04-29T15:00:00.000Z'),
      triggerSource: 'test_zero_rows'
    })
  );

  assert.equal(result.succeededConnections, 1);
  assert.equal(result.recordsReceived, 0);
  assert.equal(result.aggregateRowsUpserted, 0);

  const anomalyLog = entries.find((entry) => entry.event === 'meta_ads_order_value_sync_anomaly');
  assert.ok(anomalyLog);
  assert.equal(anomalyLog.anomalyType, 'zero_rows_pulled');
  assert.equal(anomalyLog.triggerSource, 'test_zero_rows');
  assert.equal(anomalyLog.alertable, true);
});
