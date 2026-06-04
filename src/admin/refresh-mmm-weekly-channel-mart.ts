import { pool, withTransaction } from '../db/pool.js';
import { refreshWeeklyMmmChannelInputMartWithClient } from '../modules/mmm/weekly-mart.js';

function readFlag(name: string): string | null {
  const prefixed = `--${name}`;
  const index = process.argv.indexOf(prefixed);
  if (index === -1) {
    return null;
  }

  return process.argv[index + 1] ?? null;
}

function readListFlag(name: string): string[] | undefined {
  const value = readFlag(name)?.trim();
  if (!value) {
    return undefined;
  }

  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
}

async function main() {
  const startDate = readFlag('start-date')?.trim() || process.env.MMM_WEEKLY_START_DATE?.trim();
  const endDate = readFlag('end-date')?.trim() || process.env.MMM_WEEKLY_END_DATE?.trim();
  if (!startDate || !endDate) {
    throw new Error('Usage: npm run mmm:refresh-weekly -- --start-date YYYY-MM-DD --end-date YYYY-MM-DD');
  }

  const summary = await withTransaction((client) =>
    refreshWeeklyMmmChannelInputMartWithClient(client, {
      startDate,
      endDate,
      attributionModels: readListFlag('attribution-models') ?? readListFlag('attribution-model')
    })
  );

  process.stdout.write(
    `${JSON.stringify(
      {
        martVersion: 'mmm_weekly_channel_input_mart_v1',
        startDate,
        endDate,
        qualitySummary: summary
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
