import {
	RECOVERY_SOURCE_PRECEDENCE,
	type RecoveryJobType as ContractRecoveryJobType,
} from "../../../packages/attribution-schema/index.js";
import {
	backfillCampaignMetadataHistory,
	refreshCampaignMetadataFromApis,
} from "../ad-platform-metadata-refresh/index.js";
import { backfillRecentOrdersWithRecoveredAttribution } from "../attribution/backfill.js";
import {
	GA4_SESSION_ATTRIBUTION_PIPELINE,
} from "../attribution/ga4-session-attribution.js";
import {
	assertGa4BigQueryIngestionConfig,
	type Ga4BigQueryIngestionConfig,
} from "../attribution/ga4-bigquery-config.js";
import { createGa4BigQueryExecutor } from "../attribution/ga4-bigquery-executor.js";
import {
	listHourlyRange,
	processGa4SessionAttributionHourlyJobs,
} from "../attribution/ga4-ingestion-jobs.js";
import {
	GA4_FALLBACK_RECOVERY_JOB_TYPE,
	executeGa4FallbackRecoveryRun,
} from "../attribution/ga4-fallback-recovery.js";
import {
	SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE,
	executeShopifyAttributionRecoveryRun,
} from "../attribution/shopify-hint-recovery.js";
import { reimportShopifyOrdersForDateRange } from "../shopify/index.js";
import {
	PostgresRecoveryJobStore,
	type NormalizedRecoveryError,
	type RecoveryCheckpoint,
	type RecoveryExecutionResult,
	type RecoveryRun,
	type RecoveryRunCounters,
} from "./index.js";

export const CAMPAIGN_METADATA_API_REFRESH_JOB_TYPE =
	"campaign_metadata_api_refresh";
export const CAMPAIGN_METADATA_HISTORY_BACKFILL_JOB_TYPE =
	"campaign_metadata_history_backfill";
export const GA4_SESSION_ENRICHMENT_BACKFILL_JOB_TYPE =
	"ga4_session_enrichment_backfill";
export const ORDER_ATTRIBUTION_BACKFILL_JOB_TYPE = "order_attribution_backfill";
export const SHOPIFY_ORDER_REIMPORT_JOB_TYPE = "shopify_order_reimport";

export type RegisteredRecoveryJobType =
	| ContractRecoveryJobType
	| typeof GA4_SESSION_ENRICHMENT_BACKFILL_JOB_TYPE;

export type RegisteredRecoveryJobMetadata = {
	jobType: RegisteredRecoveryJobType;
	defaultScopeKey: string;
	optionalParameters?: string[];
};

type ExecuteSingleRunInput = {
	runId: string;
	workerId: string;
	now: Date;
	store?: PostgresRecoveryJobStore;
	managesCompletion?: boolean;
	execute: (run: RecoveryRun) => Promise<{
		status: "succeeded" | "partial_failure";
		report: RecoveryCheckpoint;
		counters: Partial<RecoveryRunCounters>;
		recordsProcessed?: number;
		checkpoint?: RecoveryCheckpoint;
	}>;
};

export type ExecuteRegisteredRecoveryRunOptions = {
	managesCompletion?: boolean;
};

const registeredJobMetadata: RegisteredRecoveryJobMetadata[] = [
	{
		jobType: SHOPIFY_ORDER_REIMPORT_JOB_TYPE,
		defaultScopeKey: "shopify-order-reimport",
	},
	{
		jobType: SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE,
		defaultScopeKey: "shopify-attribution-hints",
		optionalParameters: ["chunkSize", "pageSize"],
	},
	{
		jobType: GA4_FALLBACK_RECOVERY_JOB_TYPE,
		defaultScopeKey: "ga4-fallback-unattributed",
		optionalParameters: ["chunkSize", "pageSize", "lookbackDays"],
	},
	{
		jobType: CAMPAIGN_METADATA_API_REFRESH_JOB_TYPE,
		defaultScopeKey: "campaign-metadata-api-refresh",
		optionalParameters: [
			"campaignIds",
			"platforms",
			"maxAttempts",
			"startDate",
			"endDate",
		],
	},
	{
		jobType: CAMPAIGN_METADATA_HISTORY_BACKFILL_JOB_TYPE,
		defaultScopeKey: "campaign-metadata-history",
		optionalParameters: ["unresolvedSampleLimit", "startDate", "endDate"],
	},
	{
		jobType: GA4_SESSION_ENRICHMENT_BACKFILL_JOB_TYPE,
		defaultScopeKey: "ga4-session-enrichment",
		optionalParameters: [
			"batchSize",
			"maxRetries",
			"initialBackoffSeconds",
			"maxBackoffSeconds",
			"staleLockMinutes",
			"pipelineName",
		],
	},
	{
		jobType: ORDER_ATTRIBUTION_BACKFILL_JOB_TYPE,
		defaultScopeKey: "order-attribution-backfill",
		optionalParameters: ["limit", "onlyWebOrders", "skipShopifyWriteback"],
	},
];

const registeredJobTypes = new Set<string>(
	registeredJobMetadata.map((job) => job.jobType),
);

function isObject(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function toRecoveryCheckpoint(value: unknown): RecoveryCheckpoint {
	return JSON.parse(JSON.stringify(value ?? {})) as RecoveryCheckpoint;
}

function normalizeError(error: unknown, fallbackCode: string): NormalizedRecoveryError {
	const code =
		isObject(error) && typeof error.code === "string" && error.code.trim()
			? error.code.trim().slice(0, 128)
			: error instanceof Error && error.name.trim()
				? error.name.trim().slice(0, 128)
				: fallbackCode;
	const message = error instanceof Error ? error.message : String(error);

	return {
		code,
		message: message.slice(0, 2048),
		details: toRecoveryCheckpoint({
			name: error instanceof Error ? error.name : null,
			stack: error instanceof Error ? error.stack ?? null : null,
		}),
	};
}

function getString(
	parameters: RecoveryCheckpoint,
	key: string,
): string | undefined {
	const value = parameters[key];
	return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function getBoolean(
	parameters: RecoveryCheckpoint,
	key: string,
	fallback: boolean,
): boolean {
	const value = parameters[key];
	return typeof value === "boolean" ? value : fallback;
}

function getPositiveInteger(
	parameters: RecoveryCheckpoint,
	key: string,
): number | undefined {
	const value = parameters[key];
	if (typeof value !== "number" || !Number.isFinite(value)) {
		return undefined;
	}

	const normalized = Math.trunc(value);
	return normalized > 0 ? normalized : undefined;
}

function getStringArray(parameters: RecoveryCheckpoint, key: string): string[] {
	const value = parameters[key];
	if (!Array.isArray(value)) {
		return [];
	}

	return value
		.filter((entry): entry is string => typeof entry === "string")
		.map((entry) => entry.trim())
		.filter(Boolean);
}

function dateOnly(value: string): string {
	return new Date(value).toISOString().slice(0, 10);
}

function hourStart(value: string): string {
	const parsed = new Date(value);
	return new Date(
		Date.UTC(
			parsed.getUTCFullYear(),
			parsed.getUTCMonth(),
			parsed.getUTCDate(),
			parsed.getUTCHours(),
		),
	).toISOString();
}

function getCampaignDateRange(run: RecoveryRun): {
	startDate: string | null;
	endDate: string | null;
} {
	const startDate = getString(run.inputParameters, "startDate");
	const endDate = getString(run.inputParameters, "endDate");

	return {
		startDate: startDate ?? dateOnly(run.timeRangeStart),
		endDate: endDate ?? dateOnly(run.timeRangeEnd),
	};
}

async function executeSingleRecoveryRun(
	input: ExecuteSingleRunInput,
): Promise<RecoveryExecutionResult> {
	const store = input.store ?? new PostgresRecoveryJobStore();
	const managesCompletion = input.managesCompletion ?? true;
	let run = await store.claimRun(input.runId, input.workerId, input.now);

	try {
		const result = await input.execute(run);
		if (result.recordsProcessed !== undefined || result.checkpoint !== undefined) {
			run = await store.updateCheckpoint(
				run.id,
				"default",
				result.checkpoint ?? {},
				result.recordsProcessed ?? 0,
				new Date(),
			);
		}
		run = await store.incrementRunCounters(run.id, result.counters, new Date());
		if (!managesCompletion) {
			return {
				run: {
					...run,
					status: result.status,
				},
				pagesProcessed: 1,
				recordsProcessed: result.recordsProcessed ?? run.recordsProcessed,
			};
		}
		run = await store.finalizeRun(run.id, result.status, null, new Date());

		return {
			run,
			pagesProcessed: 1,
			recordsProcessed: result.recordsProcessed ?? run.recordsProcessed,
		};
	} catch (error) {
		if (!managesCompletion) {
			throw error;
		}
		const failed = await store.finalizeRun(
			run.id,
			"failed",
			normalizeError(error, `${run.jobType}_failed`),
			new Date(),
		);

		return {
			run: failed,
			pagesProcessed: 1,
			recordsProcessed: run.recordsProcessed,
		};
	}
}

async function executeCampaignMetadataHistoryBackfill(
	runId: string,
	workerId: string,
	now: Date,
	options: ExecuteRegisteredRecoveryRunOptions = {},
): Promise<RecoveryExecutionResult> {
	return executeSingleRecoveryRun({
		runId,
		workerId,
		now,
		managesCompletion: options.managesCompletion,
		execute: async (run) => {
			const { startDate, endDate } = getCampaignDateRange(run);
			const report = await backfillCampaignMetadataHistory({
				requestedBy: run.initiatedBy,
				workerId,
				startDate: startDate ?? dateOnly(run.timeRangeStart),
				endDate: endDate ?? dateOnly(run.timeRangeEnd),
				dryRun: run.dryRun,
				unresolvedSampleLimit: getPositiveInteger(
					run.inputParameters,
					"unresolvedSampleLimit",
				),
				runId: run.id,
			});
			const plannedWrites = report.plannedInserts + report.plannedUpdates;

			return {
				status: report.status === "failed" ? "partial_failure" : "succeeded",
				report: toRecoveryCheckpoint({
					...report,
					sourcePrecedence: RECOVERY_SOURCE_PRECEDENCE,
				}),
				recordsProcessed: plannedWrites,
				checkpoint: { completedAt: report.completedAt },
				counters: {
					recordsDiscovered: plannedWrites,
					recordsClaimed: plannedWrites,
					recordsSucceeded: report.status === "failed" ? 0 : plannedWrites,
					recordsFailed: report.status === "failed" ? plannedWrites : 0,
					sideEffectsAttempted: run.dryRun ? 0 : plannedWrites,
					sideEffectsSucceeded:
						run.dryRun || report.status === "failed" ? 0 : plannedWrites,
					sideEffectsSuppressed: run.dryRun ? plannedWrites : 0,
				},
			};
		},
	});
}

async function executeShopifyOrderReimport(
	runId: string,
	workerId: string,
	now: Date,
	options: ExecuteRegisteredRecoveryRunOptions = {},
): Promise<RecoveryExecutionResult> {
	return executeSingleRecoveryRun({
		runId,
		workerId,
		now,
		managesCompletion: options.managesCompletion,
		execute: async (run) => {
			const startDate =
				getString(run.inputParameters, "startDate") ?? dateOnly(run.timeRangeStart);
			const endDate =
				getString(run.inputParameters, "endDate") ?? dateOnly(run.timeRangeEnd);

			if (run.dryRun) {
				return {
					status: "succeeded",
					report: toRecoveryCheckpoint({
						shopDomain: null,
						startDate,
						endDate,
						importedOrders: 0,
						ordersInserted: 0,
						ordersUpdated: 0,
						payloadsRefreshed: 0,
						payloadsUnchanged: 0,
						duplicateReceipts: 0,
						dryRun: true,
						sourcePrecedence: RECOVERY_SOURCE_PRECEDENCE,
					}),
					recordsProcessed: 0,
					checkpoint: { completedAt: new Date().toISOString() },
					counters: {
						sideEffectsSuppressed: 1,
					},
				};
			}

			const report = await reimportShopifyOrdersForDateRange({
				startDate,
				endDate,
			});

			return {
				status: "succeeded",
				report: toRecoveryCheckpoint({
					...report,
					sourcePrecedence: RECOVERY_SOURCE_PRECEDENCE,
				}),
				recordsProcessed: report.importedOrders,
				checkpoint: { completedAt: new Date().toISOString() },
				counters: {
					recordsDiscovered: report.importedOrders,
					recordsClaimed: report.importedOrders,
					recordsSucceeded: report.ordersInserted + report.ordersUpdated,
					recordsSkipped: report.duplicateReceipts,
					sideEffectsAttempted: report.importedOrders,
					sideEffectsSucceeded:
						report.ordersInserted +
						report.ordersUpdated +
						report.payloadsRefreshed,
				},
			};
		},
	});
}

async function executeCampaignMetadataApiRefresh(
	runId: string,
	workerId: string,
	now: Date,
	options: ExecuteRegisteredRecoveryRunOptions = {},
): Promise<RecoveryExecutionResult> {
	return executeSingleRecoveryRun({
		runId,
		workerId,
		now,
		managesCompletion: options.managesCompletion,
		execute: async (run) => {
			const { startDate, endDate } = getCampaignDateRange(run);
			const report = await refreshCampaignMetadataFromApis({
				requestedBy: run.initiatedBy,
				workerId,
				startDate,
				endDate,
				campaignIds: getStringArray(run.inputParameters, "campaignIds"),
				platforms: getStringArray(run.inputParameters, "platforms") as Array<
					"google_ads" | "meta_ads"
				>,
				dryRun: run.dryRun,
				runId: run.id,
				maxAttempts: getPositiveInteger(run.inputParameters, "maxAttempts"),
			});
			const attempted = report.platformProgress.reduce(
				(sum, progress) => sum + progress.attempted,
				0,
			);
			const failed = report.platformProgress.reduce(
				(sum, progress) => sum + progress.failed,
				0,
			);
			const refreshed = report.platformProgress.reduce(
				(sum, progress) => sum + progress.refreshed,
				0,
			);
			const skipped = report.platformProgress.reduce(
				(sum, progress) => sum + progress.skipped,
				0,
			);
			const recordCount = report.platformProgress.reduce(
				(sum, progress) => sum + progress.recordCount,
				0,
			);

			return {
				status: failed > 0 ? "partial_failure" : "succeeded",
				report: toRecoveryCheckpoint({
					...report,
					sourcePrecedence: RECOVERY_SOURCE_PRECEDENCE,
				}),
				recordsProcessed: attempted,
				checkpoint: { completedAt: report.completedAt },
				counters: {
					recordsDiscovered: report.totalCampaignReferences,
					recordsClaimed: attempted,
					recordsSucceeded: refreshed,
					recordsFailed: failed,
					recordsSkipped: skipped,
					sideEffectsAttempted: run.dryRun ? 0 : attempted,
					sideEffectsSucceeded: run.dryRun ? 0 : recordCount,
					sideEffectsSuppressed: run.dryRun ? attempted : 0,
				},
			};
		},
	});
}

async function executeGa4SessionEnrichmentBackfill(
	runId: string,
	workerId: string,
	now: Date,
	config?: Ga4BigQueryIngestionConfig,
	options: ExecuteRegisteredRecoveryRunOptions = {},
): Promise<RecoveryExecutionResult> {
	return executeSingleRecoveryRun({
		runId,
		workerId,
		now,
		managesCompletion: options.managesCompletion,
		execute: async (run) => {
			const effectiveConfig = config ?? assertGa4BigQueryIngestionConfig();
			if (!effectiveConfig.enabled) {
				throw new Error("GA4 BigQuery ingestion is disabled");
			}
			const explicitHourStarts = listHourlyRange(
				hourStart(run.timeRangeStart),
				hourStart(run.timeRangeEnd),
			);

			if (run.dryRun) {
				return {
					status: "succeeded",
					report: toRecoveryCheckpoint({
						pipelineName: GA4_SESSION_ATTRIBUTION_PIPELINE,
						explicitHourStarts,
						dryRun: true,
						sourcePrecedence: RECOVERY_SOURCE_PRECEDENCE,
					}),
					recordsProcessed: explicitHourStarts.length,
					checkpoint: { lastHourStart: explicitHourStarts.at(-1) ?? null },
					counters: {
						recordsDiscovered: explicitHourStarts.length,
						recordsClaimed: explicitHourStarts.length,
						recordsSkipped: explicitHourStarts.length,
						sideEffectsSuppressed: explicitHourStarts.length,
					},
				};
			}

			const result = await processGa4SessionAttributionHourlyJobs({
				pipelineName:
					getString(run.inputParameters, "pipelineName") ??
					GA4_SESSION_ATTRIBUTION_PIPELINE,
				requestedBy: run.initiatedBy,
				workerId,
				config: effectiveConfig,
				executor: createGa4BigQueryExecutor(effectiveConfig.ga4.location),
				batchSize: getPositiveInteger(run.inputParameters, "batchSize"),
				maxRetries: getPositiveInteger(run.inputParameters, "maxRetries"),
				initialBackoffSeconds: getPositiveInteger(
					run.inputParameters,
					"initialBackoffSeconds",
				),
				maxBackoffSeconds: getPositiveInteger(
					run.inputParameters,
					"maxBackoffSeconds",
				),
				staleLockMinutes: getPositiveInteger(
					run.inputParameters,
					"staleLockMinutes",
				),
				explicitHourStarts,
			});

			return {
				status: result.deadLetteredJobs > 0 ? "partial_failure" : "succeeded",
				report: toRecoveryCheckpoint({
					...result,
					explicitHourStarts,
					sourcePrecedence: RECOVERY_SOURCE_PRECEDENCE,
				}),
				recordsProcessed: result.claimedHourCount,
				checkpoint: { lastHourStart: explicitHourStarts.at(-1) ?? null },
				counters: {
					recordsDiscovered: explicitHourStarts.length,
					recordsClaimed: result.claimedHourCount,
					recordsSucceeded: result.succeededJobs,
					recordsFailed: result.deadLetteredJobs,
					recordsRetried: result.retriedJobs,
					sideEffectsAttempted: result.claimedHourCount,
					sideEffectsSucceeded: result.succeededJobs,
				},
			};
		},
	});
}

async function executeOrderAttributionBackfill(
	runId: string,
	workerId: string,
	now: Date,
	options: ExecuteRegisteredRecoveryRunOptions = {},
): Promise<RecoveryExecutionResult> {
	return executeSingleRecoveryRun({
		runId,
		workerId,
		now,
		managesCompletion: options.managesCompletion,
		execute: async (run) => {
			const report = await backfillRecentOrdersWithRecoveredAttribution({
				requestedBy: run.initiatedBy,
				workerId,
				windowStart: new Date(run.timeRangeStart),
				windowEnd: new Date(run.timeRangeEnd),
				limit: getPositiveInteger(run.inputParameters, "limit"),
				dryRun: run.dryRun,
				onlyWebOrders: getBoolean(run.inputParameters, "onlyWebOrders", true),
				writeToShopifyWhenAvailable: !getBoolean(
					run.inputParameters,
					"skipShopifyWriteback",
					false,
				),
				runId: run.id,
			});
			const failed =
				report.failedOrders + report.shopifyWritebackFailed;
			const sideEffectsAttempted = run.dryRun
				? 0
				: report.recoveredOrders +
					report.shopifyWritebackCompleted +
					report.shopifyWritebackSkipped +
					report.shopifyWritebackFailed;

			return {
				status: failed > 0 ? "partial_failure" : "succeeded",
				report: toRecoveryCheckpoint({
					...report,
					sourcePrecedence: RECOVERY_SOURCE_PRECEDENCE,
				}),
				recordsProcessed: report.scannedOrders,
				checkpoint: { completedAt: new Date().toISOString() },
				counters: {
					recordsDiscovered: report.beforeMetrics.ordersMissingAttribution,
					recordsClaimed: report.scannedOrders,
					recordsSucceeded: report.recoveredOrders,
					recordsFailed: failed,
					recordsSkipped: report.unrecoverableOrders,
					sideEffectsAttempted,
					sideEffectsSucceeded: run.dryRun
						? 0
						: report.recoveredOrders + report.shopifyWritebackCompleted,
					sideEffectsSuppressed: run.dryRun ? report.recoverableOrders : 0,
				},
			};
		},
	});
}

export function getRegisteredRecoveryJobTypes(): RegisteredRecoveryJobMetadata[] {
	return registeredJobMetadata;
}

export function isRegisteredRecoveryJobType(
	jobType: string,
): jobType is RegisteredRecoveryJobType {
	return registeredJobTypes.has(jobType);
}

export function getDefaultRecoveryScopeKey(jobType: string): string {
	return (
		registeredJobMetadata.find((job) => job.jobType === jobType)?.defaultScopeKey ??
		"global"
	);
}

export async function executeRegisteredRecoveryRun(
	run: Pick<RecoveryRun, "id" | "jobType">,
	workerId: string,
	now = new Date(),
	options: ExecuteRegisteredRecoveryRunOptions = {},
): Promise<RecoveryExecutionResult> {
	switch (run.jobType) {
		case SHOPIFY_ORDER_REIMPORT_JOB_TYPE:
			return executeShopifyOrderReimport(run.id, workerId, now, options);
		case SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE:
			return executeShopifyAttributionRecoveryRun(run.id, workerId, now, options);
		case GA4_FALLBACK_RECOVERY_JOB_TYPE:
			return executeGa4FallbackRecoveryRun(run.id, workerId, now, options);
		case CAMPAIGN_METADATA_API_REFRESH_JOB_TYPE:
			return executeCampaignMetadataApiRefresh(run.id, workerId, now, options);
		case CAMPAIGN_METADATA_HISTORY_BACKFILL_JOB_TYPE:
			return executeCampaignMetadataHistoryBackfill(run.id, workerId, now, options);
		case GA4_SESSION_ENRICHMENT_BACKFILL_JOB_TYPE:
			return executeGa4SessionEnrichmentBackfill(
				run.id,
				workerId,
				now,
				undefined,
				options,
			);
		case ORDER_ATTRIBUTION_BACKFILL_JOB_TYPE:
			return executeOrderAttributionBackfill(run.id, workerId, now, options);
		default:
			throw new Error(`Unsupported recovery job type: ${run.jobType}`);
	}
}
