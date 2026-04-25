import { readdirSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const TEST_DIRECTORY = 'test';
const TEST_FILE_PATTERN = /\.test\.[cm]?[jt]s$/;
const INTEGRATION_TEST_PATTERN = /\.integration\.test\.[cm]?[jt]s$/;
const VALID_MODES = new Set(['all', 'unit', 'integration']);

const mode = process.argv[2] ?? 'all';

if (!VALID_MODES.has(mode)) {
  console.error(`Unsupported test mode "${mode}". Use one of: ${[...VALID_MODES].join(', ')}`);
  process.exit(1);
}

const allTests = readdirSync(TEST_DIRECTORY)
  .filter((entry) => TEST_FILE_PATTERN.test(entry))
  .sort();

const integrationTests = allTests
  .filter((entry) => INTEGRATION_TEST_PATTERN.test(entry))
  .map((entry) => join(TEST_DIRECTORY, entry));

const unitTests = allTests
  .filter((entry) => !INTEGRATION_TEST_PATTERN.test(entry))
  .map((entry) => join(TEST_DIRECTORY, entry));

function runCommand(command, args) {
  const result = spawnSync(command, args, { stdio: 'inherit' });

  if (result.error) {
    throw result.error;
  }

  return result.status ?? 1;
}

function runNodeTests(testFiles, { serial = false } = {}) {
  if (testFiles.length === 0) {
    return 0;
  }

  const args = ['tsx', '--test'];

  if (serial) {
    args.push('--test-concurrency=1');
  }

  args.push(...testFiles);
  return runCommand('npx', args);
}

function runMigrationsIfNeeded() {
  if (mode === 'unit') {
    return 0;
  }

  return runCommand('npm', ['run', 'db:migrate']);
}

function main() {
  if (mode === 'unit') {
    process.exit(runNodeTests(unitTests));
  }

  if (mode === 'integration') {
    const migrationStatus = runMigrationsIfNeeded();

    if (migrationStatus !== 0) {
      process.exit(migrationStatus);
    }

    process.exit(runNodeTests(integrationTests, { serial: true }));
  }

  const unitStatus = runNodeTests(unitTests);

  if (unitStatus !== 0) {
    process.exit(unitStatus);
  }

  const migrationStatus = runMigrationsIfNeeded();

  if (migrationStatus !== 0) {
    process.exit(migrationStatus);
  }

  process.exit(runNodeTests(integrationTests, { serial: true }));
}

main();
