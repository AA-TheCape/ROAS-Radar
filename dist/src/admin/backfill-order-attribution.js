import { backfillRecentOrdersWithRecoveredAttribution } from '../modules/attribution/backfill.js';
function readFlag(name) {
    const prefixed = `--${name}`;
    const index = process.argv.indexOf(prefixed);
    if (index === -1) {
        return null;
    }
    return process.argv[index + 1] ?? null;
}
function requireFlag(name) {
    const value = readFlag(name)?.trim();
    if (!value) {
        throw new Error(`Missing required flag --${name}`);
    }
    return value;
}
function parseIsoDate(name, value) {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        throw new Error(`Invalid ${name} value: ${value}`);
    }
    return parsed;
}
function parseLimit(value) {
    if (!value) {
        return undefined;
    }
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid limit value: ${value}`);
    }
    return parsed;
}
async function run() {
    const report = await backfillRecentOrdersWithRecoveredAttribution({
        windowStart: parseIsoDate('from', requireFlag('from')),
        windowEnd: parseIsoDate('to', requireFlag('to')),
        requestedBy: requireFlag('requested-by'),
        workerId: readFlag('worker-id')?.trim() || 'order-attribution-backfill',
        limit: parseLimit(readFlag('limit')),
        dryRun: process.argv.includes('--dry-run'),
        onlyWebOrders: !process.argv.includes('--include-non-web-orders'),
        writeToShopifyWhenAvailable: !process.argv.includes('--skip-shopify-writeback')
    });
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
run().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
});
