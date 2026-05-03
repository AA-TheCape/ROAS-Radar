function buildQueueMetricsLog(result: MetaAdsQueueProcessResult): void {
  logInfo('meta_ads_sync_run', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    pipeline: 'meta_ads_spend',
    runtimeMode: 'spend',
    workerId: result.workerId,
    enqueuedJobs: result.enqueuedJobs,
    claimedJobs: result.claimedJobs,
    succeededJobs: result.succeededJobs,
    failedJobs: result.failedJobs,
    durationMs: result.durationMs
  });
}

logInfo('meta_ads_order_value_sync_completed', {
  service: process.env.K_SERVICE ?? 'roas-radar',
  pipeline: 'meta_ads_order_value',
  runtimeMode: 'order_value',
  triggerSource,
  succeededConnections: result.succeededConnections,
  failedConnections: result.failedConnections
});
