import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const testDir = path.join(repoRoot, 'test');
const mode = process.argv[2] ?? 'all';

const integrationTests = new Set([
  'attribution-e2e.integration.test.ts',
  'reporting-aggregates.integration.test.ts',
  'reporting-api.integration.test.ts'
]);
const unitTests = [];
const selectedTests = [];

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

for (const file of readdirSync(testDir).sort()) {
  if (!file.endsWith('.test.ts') && !file.endsWith('.test.js')) {
    continue;
  }

  if (integrationTests.has(file)) {
    continue;
  }

  unitTests.push(path.join('test', file));
}

if (mode === 'unit') {
  selectedTests.push(...unitTests);
} else if (mode === 'integration') {
  selectedTests.push(...Array.from(integrationTests, (file) => path.join('test', file)));
} else if (mode === 'all') {
  selectedTests.push(...unitTests);
  selectedTests.push(...Array.from(integrationTests, (file) => path.join('test', file)));
} else {
  process.stderr.write(`Unknown test mode: ${mode}\n`);
  process.exit(1);
}

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

const result = spawnSync('npx', ['tsx', '--test', ...selectedTests], {
  cwd: repoRoot,
  stdio: 'inherit',
  env: process.env
});

if (typeof result.status === 'number') {
  process.exit(result.status);
}

process.exit(1);
