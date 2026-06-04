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

function readNumberEnv(name: string): number | undefined {
  const value = process.env[name]?.trim();
  if (!value) {
    return undefined;
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error(`${name} must be numeric`);
  }

  return numeric;
}

function toDateString(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addUtcDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function resolveTrainingWindow() {
  const explicitStartDate = readFlag('start-date')?.trim() || process.env.MMM_BASELINE_START_DATE?.trim();
  const explicitEndDate = readFlag('end-date')?.trim() || process.env.MMM_BASELINE_END_DATE?.trim();

  if (explicitStartDate && explicitEndDate) {
    return {
      startDate: explicitStartDate,
      endDate: explicitEndDate
    };
  }

  const lookbackDays = readNumberEnv('MMM_BASELINE_LOOKBACK_DAYS');
  const lagDays = readNumberEnv('MMM_BASELINE_LAG_DAYS') ?? 1;
  if (lookbackDays && lookbackDays > 0) {
    const today = new Date();
    const endDate = addUtcDays(today, -lagDays);
    const startDate = addUtcDays(endDate, -(Math.trunc(lookbackDays) - 1));
    return {
      startDate: toDateString(startDate),
      endDate: toDateString(endDate)
    };
  }

  throw new Error(
    'Usage: npm run mmm:train-baseline -- --start-date YYYY-MM-DD --end-date YYYY-MM-DD, or set MMM_BASELINE_LOOKBACK_DAYS'
  );
}

async function main() {
  const { startDate, endDate } = resolveTrainingWindow();

  const run = await trainBaselineMmmModel({
    startDate,
    endDate,
    attributionModel: readFlag('attribution-model')?.trim() || process.env.MMM_BASELINE_ATTRIBUTION_MODEL?.trim() || undefined,
    maxSegments: readNumberFlag('max-segments') ?? readNumberEnv('MMM_BASELINE_MAX_SEGMENTS'),
    adstockDecay: readNumberFlag('adstock-decay') ?? readNumberEnv('MMM_BASELINE_ADSTOCK_DECAY'),
    ridgeLambda: readNumberFlag('ridge-lambda') ?? readNumberEnv('MMM_BASELINE_RIDGE_LAMBDA'),
    holdoutRatio: readNumberFlag('holdout-ratio') ?? readNumberEnv('MMM_BASELINE_HOLDOUT_RATIO'),
    submittedBy: readFlag('submitted-by')?.trim() || process.env.MMM_BASELINE_SUBMITTED_BY?.trim() || 'admin-cli'
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
