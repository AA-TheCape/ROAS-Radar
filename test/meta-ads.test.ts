test('summarizeOrderValueRecords counts null canonical fields and selection modes', () => {
  const summary = __metaAdsTestUtils.summarizeOrderValueRecords([
    // priority row
    // no-selection row
    // fallback row
  ]);

  assert.deepEqual(summary, {
    totalRows: 3,
    nullAttributedRevenueCount: 2,
    nullPurchaseCountCount: 1,
    nullActionTypeCount: 1,
    fallbackSelectionCount: 1,
    prioritySelectionCount: 1,
    noSelectionCount: 1
  });
});

test('buildOrderValueSyncAnomalies detects zero-row pulls and sudden null-rate spikes against the baseline', () => {
  const summary = __metaAdsTestUtils.summarizeOrderValueRecords(records);
  const anomalies = __metaAdsTestUtils.buildOrderValueSyncAnomalies({
    rawRowsFetched: 0,
    records,
    summary,
    baseline: {
      totalRows: 5,
      nullAttributedRevenueCount: 0,
      nullPurchaseCountCount: 0,
      nullActionTypeCount: 0
    }
  });

  assert.deepEqual(
    anomalies.map((anomaly: { type: string }) => anomaly.type),
    [
      'zero_rows_pulled',
      'null_attributed_revenue_spike',
      'null_purchase_count_spike',
      'null_action_type_spike'
    ]
  );
});
