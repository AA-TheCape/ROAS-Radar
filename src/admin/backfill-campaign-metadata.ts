import { backfillCampaignMetadataHistory } from '../modules/ad-platform-metadata-refresh/index.js';

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

function parseBooleanFlag(name: string): boolean {
  const value = readFlag(name)?.trim().toLowerCase();
  return value === '1' || value === 'true' || value === 'yes' || value === 'on';
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

async function run(): Promise<void> {
  const report = await backfillCampaignMetadataHistory({
    requestedBy: requireFlag('requested-by'),
    workerId: readFlag('worker-id')?.trim() || 'campaign-metadata-backfill',
    startDate: requireFlag('start-date'),
    endDate: requireFlag('end-date'),
    dryRun: parseBooleanFlag('dry-run'),
    unresolvedSampleLimit: parseOptionalPositiveInteger('unresolved-sample-limit'),
    runId: readFlag('run-id')?.trim() || null
  });

  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

run().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
