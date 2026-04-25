import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const testDir = path.join(repoRoot, 'test');
const mode = process.argv[2] ?? 'all';
const validModes = new Set(['all', 'unit', 'integration']);
const integrationTestPattern = /\.integration\.test\.[cm]?[jt]s$/;
const explicitIntegrationTests = new Set(['dead-letter-replay.test.ts', 'shopify-writeback.test.ts']);

if (!validModes.has(mode)) {
  process.stderr.write(`Unknown test mode: ${mode}\n`);
  process.exit(1);
}

function ensureDashboardDependencies() {
  const dashboardNodeModules = path.join(repoRoot, 'dashboard', 'node_modules');

  if (existsSync(dashboardNodeModules)) {
    return;
  }

  const installResult = spawnSync('npm', ['--prefix', 'dashboard', 'ci'], {
    cwd: repoRoot,
    stdio: 'inherit',
    env: process.env
  });

  if (installResult.status !== 0) {
    process.exit(installResult.status ?? 1);
  }
}

function runMigrations() {
  const migrationResult = spawnSync('npx', ['tsx', 'src/db/migrate.ts'], {
    cwd: repoRoot,
    stdio: 'inherit',
    env: process.env
  });

  if (migrationResult.status !== 0) {
    process.exit(migrationResult.status ?? 1);
  }
}

const allTests = readdirSync(testDir)
  .filter((file) => file.endsWith('.test.ts') || file.endsWith('.test.js'))
  .sort();

const integrationTests = allTests
  .filter((file) => integrationTestPattern.test(file) || explicitIntegrationTests.has(file))
  .map((file) => path.join('test', file));

const unitTests = allTests
  .filter((file) => !integrationTestPattern.test(file) && !explicitIntegrationTests.has(file))
  .map((file) => path.join('test', file));

const selectedTests =
  mode === 'unit'
    ? unitTests
    : mode === 'integration'
      ? integrationTests
      : [...unitTests, ...integrationTests];

if (selectedTests.length === 0) {
  process.stderr.write(`No tests matched mode ${mode}\n`);
  process.exit(1);
}

if (selectedTests.some((file) => /authenticated-ui|dashboard-ui/.test(file))) {
  ensureDashboardDependencies();
}

if (mode === 'integration' || mode === 'all') {
  runMigrations();
}

const args = ['tsx', '--test'];

if (mode === 'integration') {
  args.push('--test-concurrency=1');
}

args.push(...selectedTests);

const result = spawnSync('npx', args, {
  cwd: repoRoot,
  stdio: 'inherit',
  env: process.env
});

if (typeof result.status === 'number') {
  process.exit(result.status);
}

process.exit(1);
