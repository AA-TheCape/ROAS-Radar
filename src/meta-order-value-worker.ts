const runtime = resolveMetaAdsRuntimeDescriptor("order_value");

logInfo("meta_ads_order_value_worker_started", {
	service: runtime.service,
	jobName: runtime.jobName,
	jobExecution: runtime.jobExecution,
	triggerSource: runtime.triggerSource,
	runtimeMode: runtime.runtimeMode,
	pipeline: runtime.pipeline,
});

const result = await runMetaAdsOrderValueSync({
	triggerSource: runtime.triggerSource
});
