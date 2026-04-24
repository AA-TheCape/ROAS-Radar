import { replayDeadLetters } from '../modules/dead-letters/index.js';

async function run(): Promise<void> {
  const result = await replayDeadLetters({
    requestedBy: requireFlag('requested-by'),
    eventType: readFlag('event-type') ?? undefined,
    sourceTable: readFlag('source-table') ?? undefined,
    windowStart: optionalDate(readFlag('from')),
    windowEnd: optionalDate(readFlag('to')),
    limit: parsedLimit,
    dryRun: process.argv.includes('--dry-run')
  });

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}
