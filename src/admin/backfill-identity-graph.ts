import { backfillHistoricalIdentityGraph } from '../modules/identity/backfill.js';

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

function parseOptionalDate(name: string): Date | null {
  const value = readFlag(name)?.trim();

  if (!value) {
    return null;
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }

  return parsed;
}

function parseOptionalPositiveInteger(name: string): number | undefined {
  const value = readFlag(name)?.trim();

  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }

  return parsed;
}

function parseOptionalSourceList():
  | Array<'tracking_sessions' | 'tracking_events' | 'shopify_customers' | 'shopify_orders'>
  | undefined {
  const value = readFlag('sources')?.trim();

  if (!value) {
    return undefined;
  }

  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean) as Array<'tracking_sessions' | 'tracking_events' | 'shopify_customers' | 'shopify_orders'>;
}

async function run(): Promise<void> {
  const report = await backfillHistoricalIdentityGraph({
    requestedBy: requireFlag('requested-by'),
    workerId: readFlag('worker-id')?.trim() || 'identity-graph-backfill',
    runId: readFlag('run-id')?.trim() || null,
    startAt: parseOptionalDate('start-at'),
    endAt: parseOptionalDate('end-at'),
    batchSize: parseOptionalPositiveInteger('batch-size'),
    sources: parseOptionalSourceList()
  });

  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

run().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
