import { randomUUID } from "node:crypto";

import { listHourlyRange } from "./ga4-ingestion-jobs.js";

function parseOptionalInteger(name: string): number | undefined {
	const value = process.env[name]?.trim();

	if (!value) {
		return undefined;
	}

	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		throw new Error(`Invalid ${name} value: ${value}`);
	}

	return parsed;
}

function parseOptionalHour(name: string): string | undefined {
	const value = process.env[name]?.trim();

	if (!value) {
		return undefined;
	}

	const parsed = new Date(value);
	if (Number.isNaN(parsed.getTime())) {
		throw new Error(`Invalid ${name} value: ${value}`);
	}

	return new Date(
		Date.UTC(
			parsed.getUTCFullYear(),
			parsed.getUTCMonth(),
			parsed.getUTCDate(),
			parsed.getUTCHours(),
		),
	).toISOString();
}

export function resolveGa4IngestionExecution(_now: Date): {
	requestedBy: string;
	workerId: string;
	explicitHourStarts?: string[];
	batchSize: number;
	maxRetries: number;
	initialBackoffSeconds: number;
	maxBackoffSeconds: number;
	staleLockMinutes: number;
} {
	const requestedBy =
		process.env.GA4_INGESTION_REQUESTED_BY?.trim() ||
		process.env.K_SERVICE?.trim() ||
		process.env.K_JOB?.trim() ||
		"cloud-run-scheduler";
	const workerId =
		process.env.GA4_INGESTION_WORKER_ID?.trim() ||
		process.env.K_JOB_EXECUTION?.trim() ||
		`ga4-session-attribution-${randomUUID()}`;

	const explicitStartHour = parseOptionalHour("GA4_INGESTION_START_HOUR");
	const explicitEndHour =
		parseOptionalHour("GA4_INGESTION_END_HOUR") ?? explicitStartHour;
	const explicitHourStarts =
		explicitStartHour && explicitEndHour
			? listHourlyRange(explicitStartHour, explicitEndHour)
			: undefined;

	return {
		requestedBy,
		workerId,
		explicitHourStarts,
		batchSize: parseOptionalInteger("GA4_INGESTION_BATCH_SIZE") ?? 24,
		maxRetries: parseOptionalInteger("GA4_INGESTION_MAX_RETRIES") ?? 5,
		initialBackoffSeconds:
			parseOptionalInteger("GA4_INGESTION_INITIAL_BACKOFF_SECONDS") ?? 30,
		maxBackoffSeconds:
			parseOptionalInteger("GA4_INGESTION_MAX_BACKOFF_SECONDS") ?? 1_800,
		staleLockMinutes:
			parseOptionalInteger("GA4_INGESTION_STALE_LOCK_MINUTES") ?? 30,
	};
}
