import {
	buildDefaultShopifyReimportRange,
	reimportShopifyOrdersForDateRange,
} from "../modules/shopify/index.js";

function readFlag(name: string): string | null {
	const prefixed = `--${name}`;
	const index = process.argv.indexOf(prefixed);

	if (index === -1) {
		return null;
	}

	return process.argv[index + 1] ?? null;
}

function assertDateOnly(name: string, value: string): string {
	if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
		throw new Error(`Invalid ${name} value: ${value}. Use YYYY-MM-DD.`);
	}

	return value;
}

function parseRange(): { startDate: string; endDate: string } {
	const from = readFlag("from")?.trim();
	const to = readFlag("to")?.trim();

	if (!from && !to) {
		return buildDefaultShopifyReimportRange();
	}

	if (!from || !to) {
		throw new Error("Provide both --from and --to, or omit both for the last 30 days.");
	}

	const startDate = assertDateOnly("from", from);
	const endDate = assertDateOnly("to", to);

	if (startDate > endDate) {
		throw new Error("--from must be on or before --to.");
	}

	return { startDate, endDate };
}

async function run(): Promise<void> {
	const report = await reimportShopifyOrdersForDateRange(parseRange());
	process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

run().catch((error) => {
	const message = error instanceof Error ? error.message : String(error);
	process.stderr.write(`${message}\n`);
	process.exitCode = 1;
});
