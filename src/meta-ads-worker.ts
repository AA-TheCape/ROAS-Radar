const runtime = resolveMetaAdsRuntimeDescriptor("spend");

logInfo("meta_ads_spend_worker_started", {
	service: runtime.service,
	jobName: runtime.jobName,
	jobExecution: runtime.jobExecution,
	triggerSource: runtime.triggerSource,
	runtimeMode: runtime.runtimeMode,
	pipeline: runtime.pipeline,
});

const result = await processMetaAdsSyncQueue({
	workerId,
	limit: env.META_ADS_SYNC_BATCH_SIZE,
	emitMetrics: true,
	triggerSource: runtime.triggerSource,
});

logInfo("meta_ads_spend_worker_completed", {
	service: runtime.service,
	pipeline: runtime.pipeline,
	runtimeMode: runtime.runtimeMode,
	enqueuedJobs,
	claimedJobs,
	succeededJobs,
	failedJobs,
});
