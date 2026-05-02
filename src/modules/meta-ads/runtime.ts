export type MetaAdsRuntimeMode = "spend" | "order_value";

export type MetaAdsRuntimeDescriptor = {
	service: string;
	jobName: string | null;
	jobExecution: string | null;
	triggerSource: "cloud_run_job" | "manual";
	runtimeMode: MetaAdsRuntimeMode;
	pipeline: "meta_ads_spend" | "meta_ads_order_value";
};

const META_ADS_JOB_MODE_ENV = "META_ADS_JOB_MODE";

export function resolveMetaAdsRuntimeDescriptor(
	expectedRuntimeMode: MetaAdsRuntimeMode,
): MetaAdsRuntimeDescriptor {
	// Normalizes META_ADS_JOB_MODE and throws on mixed-mode execution.
	// Also derives a stable pipeline label for logs and metrics.
}
