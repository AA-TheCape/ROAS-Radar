import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";

import { pool } from "./db/pool.js";
import { backfillHistoricalIdentityGraph } from "./modules/identity/backfill.js";
import { logError, logInfo } from "./observability/index.js";

const IDENTITY_GRAPH_BACKFILL_SOURCES = [
	"tracking_sessions",
	"tracking_events",
	"shopify_customers",
	"shopify_orders",
] as const;

type IdentityGraphBackfillSource =
	(typeof IDENTITY_GRAPH_BACKFILL_SOURCES)[number];

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

function parseOptionalDate(name: string): Date | undefined {
	const value = process.env[name]?.trim();

	if (!value) {
		return undefined;
	}

	const parsed = new Date(value);

	if (Number.isNaN(parsed.getTime())) {
		throw new Error(`Invalid ${name} value: ${value}`);
	}

	return parsed;
}

function resolveSources(): IdentityGraphBackfillSource[] | undefined {
	const rawSources = process.env.IDENTITY_GRAPH_BACKFILL_SOURCES?.trim();

	if (!rawSources) {
		return undefined;
	}

	const allowedSources = new Set<string>(IDENTITY_GRAPH_BACKFILL_SOURCES);
	const sources = rawSources
		.split(",")
		.map((source) => source.trim())
		.filter(Boolean);

	if (sources.length === 0) {
		return undefined;
	}

	for (const source of sources) {
		if (!allowedSources.has(source)) {
			throw new Error(
				`Unsupported IDENTITY_GRAPH_BACKFILL_SOURCES entry: ${source}`,
			);
		}
	}

	return sources as IdentityGraphBackfillSource[];
}

export function resolveIdentityGraphBackfillExecution(now: Date): {
	requestedBy: string;
	workerId: string;
	startAt: Date;
	endAt: Date;
	batchSize?: number;
	maxBatches?: number;
	sources?: IdentityGraphBackfillSource[];
} {
	const requestedBy =
		process.env.IDENTITY_GRAPH_BACKFILL_REQUESTED_BY?.trim() ||
		"cloud-run-scheduler";
	const workerId =
		process.env.IDENTITY_GRAPH_BACKFILL_WORKER_ID?.trim() ||
		process.env.K_JOB_EXECUTION?.trim() ||
		`identity-graph-backfill-${randomUUID()}`;
	const lagHours =
		parseOptionalInteger("IDENTITY_GRAPH_BACKFILL_LAG_HOURS") ?? 1;
	const lookbackDays =
		parseOptionalInteger("IDENTITY_GRAPH_BACKFILL_LOOKBACK_DAYS") ?? 2;
	const configuredEndAt = parseOptionalDate("IDENTITY_GRAPH_BACKFILL_END_AT");
	const configuredStartAt = parseOptionalDate(
		"IDENTITY_GRAPH_BACKFILL_START_AT",
	);
	const endAt =
		configuredEndAt ?? new Date(now.getTime() - lagHours * 60 * 60 * 1000);
	const startAt =
		configuredStartAt ??
		new Date(endAt.getTime() - lookbackDays * 24 * 60 * 60 * 1000);

	if (startAt > endAt) {
		throw new Error(
			"IDENTITY_GRAPH_BACKFILL_START_AT must be on or before the resolved end time",
		);
	}

	return {
		requestedBy,
		workerId,
		startAt,
		endAt,
		batchSize: parseOptionalInteger("IDENTITY_GRAPH_BACKFILL_BATCH_SIZE"),
		maxBatches: parseOptionalInteger("IDENTITY_GRAPH_BACKFILL_MAX_BATCHES"),
		sources: resolveSources(),
	};
}

async function run(): Promise<void> {
	const execution = resolveIdentityGraphBackfillExecution(new Date());

	logInfo("identity_graph_backfill_worker_started", {
		workerId: execution.workerId,
		requestedBy: execution.requestedBy,
		startAt: execution.startAt.toISOString(),
		endAt: execution.endAt.toISOString(),
		sources: execution.sources ?? IDENTITY_GRAPH_BACKFILL_SOURCES,
		service:
			process.env.K_SERVICE ??
			process.env.K_JOB ??
			"roas-radar-identity-graph-backfill",
	});

	const report = await backfillHistoricalIdentityGraph({
		requestedBy: execution.requestedBy,
		workerId: execution.workerId,
		startAt: execution.startAt,
		endAt: execution.endAt,
		batchSize: execution.batchSize,
		maxBatches: execution.maxBatches,
		sources: execution.sources,
	});

	process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
	await pool.end();
}

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	run().catch(async (error) => {
		logError("identity_graph_backfill_worker_failed", error, {
			service:
				process.env.K_SERVICE ??
				process.env.K_JOB ??
				"roas-radar-identity-graph-backfill",
		});
		await pool.end().catch(() => undefined);
		process.exit(1);
	});
}
