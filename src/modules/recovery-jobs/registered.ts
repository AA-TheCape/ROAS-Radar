import { logInfo } from "../../observability/index.js";
import {
	executeRegisteredRecoveryRun,
	getRegisteredRecoveryJobTypes,
} from "../recovery/registered-jobs.js";
import {
	createRecoveryJobExecutor,
	type RecoveryJobDefinition,
	type RecoveryJobExecutor,
	type RecoveryJobRun,
	type RecoveryJobStore,
} from "./index.js";

export function createRegisteredRecoveryJobDefinitions(): RecoveryJobDefinition[] {
	return getRegisteredRecoveryJobTypes().map((metadata) => ({
		jobType: metadata.jobType,
		managesCompletion: true,
		run: async (context) => {
			await context.heartbeat();
			const result = await executeRegisteredRecoveryRun(
				{
					id: context.run.id,
					jobType: context.run.jobType,
				},
				context.workerId,
			);
			await context.heartbeat().catch(() => undefined);
			return {
				status: result.run.status === "partial_failure"
					? "partial_failure"
					: "succeeded",
				report: {
					pagesProcessed: result.pagesProcessed,
					recordsProcessed: result.recordsProcessed,
				},
			};
		},
	}));
}

export function createRegisteredRecoveryJobExecutor(
	store?: RecoveryJobStore,
): RecoveryJobExecutor {
	return createRecoveryJobExecutor(
		createRegisteredRecoveryJobDefinitions(),
		store,
	);
}

export async function processRegisteredRecoveryJobs(input: {
	workerId: string;
	limit?: number;
	executor?: RecoveryJobExecutor;
}): Promise<{
	recoveredStale: number;
	claimed: number;
	completed: number;
	deadLettered: number;
	lastRun: RecoveryJobRun | null;
}> {
	const executor = input.executor ?? createRegisteredRecoveryJobExecutor();
	const limit = input.limit ?? 1;
	const recovered = await executor.recoverStale();
	let claimed = 0;
	let completed = 0;
	let deadLettered = 0;
	let lastRun: RecoveryJobRun | null = null;

	for (let index = 0; index < limit; index += 1) {
		const result = await executor.executeNext({ workerId: input.workerId });
		if (!result.claimed) {
			break;
		}

		claimed += 1;
		lastRun = result.run;
		if (["succeeded", "partial_failure", "failed"].includes(result.run.status)) {
			completed += 1;
		}
		if (result.run.status === "dead_lettered") {
			deadLettered += 1;
		}
	}

	if (recovered.length > 0 || claimed > 0) {
		logInfo("recovery_job_queue_processed", {
			workerId: input.workerId,
			recoveredStale: recovered.length,
			claimed,
			completed,
			deadLettered,
			lastRunId: lastRun?.id ?? null,
			lastRunStatus: lastRun?.status ?? null,
		});
	}

	return {
		recoveredStale: recovered.length,
		claimed,
		completed,
		deadLettered,
		lastRun,
	};
}
