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
	const rawMode = process.env[META_ADS_JOB_MODE_ENV]?.trim();
	const runtimeMode = rawMode === "" || rawMode === undefined ? expectedRuntimeMode : rawMode;

	if (runtimeMode !== "spend" && runtimeMode !== "order_value") {
		throw new Error(
			`Invalid ${META_ADS_JOB_MODE_ENV}: expected "spend" or "order_value", received ${runtimeMode}`,
		);
	}

	if (runtimeMode !== expectedRuntimeMode) {
		throw new Error(
			`Meta Ads runtime mode mismatch: expected ${expectedRuntimeMode}, received ${runtimeMode}`,
		);
	}

	return {
		service:
			process.env.K_SERVICE ??
			process.env.K_JOB ??
			(runtimeMode === "spend"
				? "roas-radar-meta-ads-sync"
				: "roas-radar-meta-order-value-sync"),
		jobName: process.env.K_JOB ?? null,
		jobExecution: process.env.K_JOB_EXECUTION ?? null,
		triggerSource: process.env.K_JOB || process.env.K_JOB_EXECUTION ? "cloud_run_job" : "manual",
		runtimeMode,
		pipeline: runtimeMode === "spend" ? "meta_ads_spend" : "meta_ads_order_value",
	};
}
