import {
  backfillOrderAttributionConfidenceMetadata,
  buildEmptyOrderAttributionConfidenceBackfillProgress
} from '../modules/attribution/confidence-backfill.js';

function readFlag(name: string): string | null {
  const prefixed = `--${name}`;
  const index = process.argv.indexOf(prefixed);

  if (index === -1) {
    return null;
  }

  return process.argv[index + 1] ?? null;
}

function parsePositiveInteger(name: string, value: string | null): number | undefined {
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
  const progress = buildEmptyOrderAttributionConfidenceBackfillProgress();
  progress.cursor.lastOrderRowId = readFlag('resume-after-order-row-id')?.trim() || null;

  const report = await backfillOrderAttributionConfidenceMetadata({
    workerId: readFlag('worker-id')?.trim() || 'attribution-confidence-backfill',
    batchSize: parsePositiveInteger('batch-size', readFlag('batch-size')),
    maxRetries: parsePositiveInteger('max-retries', readFlag('max-retries')),
    dryRun: process.argv.includes('--dry-run'),
    progress
  });

  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

run().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
