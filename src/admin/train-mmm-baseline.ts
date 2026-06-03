import { pool } from '../db/pool.js';
import { trainBaselineMmmModel } from '../modules/mmm/baseline.js';

function readFlag(name: string): string | null {
  const prefixed = `--${name}`;
  const index = process.argv.indexOf(prefixed);
  if (index === -1) {
    return null;
  }

  return process.argv[index + 1] ?? null;
}

function readNumberFlag(name: string): number | undefined {
  const value = readFlag(name)?.trim();
  if (!value) {
    return undefined;
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error(`--${name} must be numeric`);
  }

  return numeric;
}

async function main() {
  const startDate = readFlag('start-date')?.trim();
  const endDate = readFlag('end-date')?.trim();
  if (!startDate || !endDate) {
    throw new Error('Usage: npm run mmm:train-baseline -- --start-date YYYY-MM-DD --end-date YYYY-MM-DD');
  }

  const run = await trainBaselineMmmModel({
    startDate,
    endDate,
    attributionModel: readFlag('attribution-model')?.trim() || undefined,
    maxSegments: readNumberFlag('max-segments'),
    adstockDecay: readNumberFlag('adstock-decay'),
    ridgeLambda: readNumberFlag('ridge-lambda'),
    holdoutRatio: readNumberFlag('holdout-ratio'),
    submittedBy: readFlag('submitted-by')?.trim() || 'admin-cli'
  });

  process.stdout.write(
    `${JSON.stringify(
      {
        id: run.id,
        modelVersion: run.modelVersion,
        attributionModel: run.attributionModel,
        trainingStartDate: run.trainingStartDate,
        trainingEndDate: run.trainingEndDate,
        inputSummary: run.inputSummary,
        validationReport: run.validationReport
      },
      null,
      2
    )}\n`
  );
}

main()
  .catch((error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
