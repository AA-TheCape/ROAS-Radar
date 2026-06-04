import { pool } from "../db/pool.js";
import { enqueueMetaDeterministicHistoricalBackfill } from "../modules/meta-ads/deterministic-events.js";

function readFlag(name: string): string | null {
	const prefixed = `--${name}`;
	const index = process.argv.indexOf(prefixed);

	if (index === -1) {
		return null;
	}

	return process.argv[index + 1] ?? null;
}

function requireFlag(name: string): string {
	const value = readFlag(name)?.trim();

	if (!value) {
		throw new Error(`Missing required flag --${name}`);
	}

	return value;
}

function parseOptionalPositiveInteger(name: string): number | undefined {
	const value = readFlag(name)?.trim();

	if (!value) {
		return undefined;
	}

	const parsed = Number.parseInt(value, 10);

	if (!Number.isInteger(parsed) || parsed <= 0) {
		throw new Error(`Invalid ${name} value: ${value}`);
	}

	return parsed;
}

async function run(): Promise<void> {
	const report = await enqueueMetaDeterministicHistoricalBackfill({
		connectionId: parseOptionalPositiveInteger("connection-id"),
		adAccountId: readFlag("ad-account-id")?.trim() || undefined,
		startDate: requireFlag("start-date"),
		endDate: requireFlag("end-date"),
		dryRun: process.argv.includes("--dry-run"),
		requestedBy: requireFlag("requested-by"),
		workerId: requireFlag("worker-id"),
		force: process.argv.includes("--force"),
	});

	process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

run()
	.catch((error) => {
		const message = error instanceof Error ? error.message : String(error);
		process.stderr.write(`${message}\n`);
		process.exitCode = 1;
	})
	.finally(async () => {
		await pool.end().catch(() => undefined);
	});
