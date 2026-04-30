async function captureStructuredLogs<T>(callback: () => Promise<T>): Promise<{
  entries: Array<Record<string, unknown>>;
  result: T;
}> {
  // captures stdout/stderr JSON logs for assertions
}

test('runMetaAdsOrderValueSync upserts current-window aggregates without duplicating rows across repeated hourly runs', async () => {
  const { entries, result: firstRun } = await captureStructuredLogs(() =>
    runMetaAdsOrderValueSync({
      now: new Date('2026-04-29T15:00:00.000Z'),
      triggerSource: 'test'
    })
  );

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
