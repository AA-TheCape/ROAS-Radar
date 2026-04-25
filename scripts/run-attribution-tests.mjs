import { spawnSync } from 'node:child_process';

const VALID_MODES = new Set(['critical', 'coverage']);

const mode = process.argv[2] ?? 'critical';

if (!VALID_MODES.has(mode)) {
  console.error(`Unsupported attribution test mode "${mode}". Use one of: ${[...VALID_MODES].join(', ')}`);
  process.exit(1);
}

const CRITICAL_TESTS = [
  'test/tracking-attribution-ingestion.test.ts',
  'test/tracking-dual-write.test.ts',
  'test/tracking-request-context-fallback.integration.test.ts',
  'test/attribution-hardening-acceptance.integration.test.ts',
  'test/attribution-backfill.integration.test.ts',
  'test/attribution-resolver.test.ts',
  'test/attribution-order-finalization.integration.test.ts',
  'test/shopify-writeback.test.ts',
  'test/dead-letter-replay.test.ts',
  'test/tracking-retention.integration.test.ts',
  'test/attribution-e2e.integration.test.ts'
];

const COVERAGE_TESTS = CRITICAL_TESTS.filter((testFile) => testFile !== 'test/attribution-e2e.integration.test.ts');

const COVERAGE_TARGETS = [
  'packages/attribution-schema/index.ts',
  'src/modules/attribution/engine.ts',
  'src/modules/attribution/index.ts',
  'src/modules/attribution/resolver.ts',
  'src/modules/dead-letters/index.ts',
  'src/modules/shopify/writeback.ts',
  'src/modules/tracking/index.ts',
  'src/modules/tracking/retention.ts'
];

const COVERAGE_THRESHOLDS = {
  line: 85,
  branch: 60,
  funcs: 75
};

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    ...options
  });

  if (result.error) {
    throw result.error;
  }

  return result;
}

function runMigrations() {
  const migrationResult = run('npm', ['run', 'db:migrate'], { stdio: 'inherit' });

  if ((migrationResult.status ?? 1) !== 0) {
    process.exit(migrationResult.status ?? 1);
  }
}

function parseCoverageReport(output) {
  const coverage = new Map();

  for (const line of output.split('\n')) {
    const match = line.match(/^#\s+(.+?)\s+\|\s+([\d.]+)\s+\|\s+([\d.]+)\s+\|\s+([\d.]+)\s+\|/);

    if (!match) {
      continue;
    }

    const [, file, linePercent, branchPercent, funcsPercent] = match;
    coverage.set(file.trim(), {
      line: Number(linePercent),
      branch: Number(branchPercent),
      funcs: Number(funcsPercent)
    });
  }

  return coverage;
}

function enforceCoverageThresholds(output) {
  const coverage = parseCoverageReport(output);
  const failures = [];

  for (const file of COVERAGE_TARGETS) {
    const metrics = coverage.get(file);

    if (!metrics) {
      failures.push(`${file}: missing from coverage report`);
      continue;
    }

    if (metrics.line < COVERAGE_THRESHOLDS.line) {
      failures.push(`${file}: line ${metrics.line}% < ${COVERAGE_THRESHOLDS.line}%`);
    }

    if (metrics.branch < COVERAGE_THRESHOLDS.branch) {
      failures.push(`${file}: branch ${metrics.branch}% < ${COVERAGE_THRESHOLDS.branch}%`);
    }

    if (metrics.funcs < COVERAGE_THRESHOLDS.funcs) {
      failures.push(`${file}: funcs ${metrics.funcs}% < ${COVERAGE_THRESHOLDS.funcs}%`);
    }
  }

  if (failures.length > 0) {
    console.error('\nAttribution coverage threshold failures:');
    for (const failure of failures) {
      console.error(`- ${failure}`);
    }
    process.exit(1);
  }
}

function runCriticalSuite() {
  runMigrations();

  const result = run(
    'npx',
    ['tsx', '--test', '--test-concurrency=1', ...CRITICAL_TESTS],
    { stdio: 'inherit' }
  );

  process.exit(result.status ?? 1);
}

function runCoverageSuite() {
  runMigrations();

  const result = run('node', [
    '--experimental-test-coverage',
    '--import',
    'tsx',
    '--test',
    '--test-concurrency=1',
    ...COVERAGE_TESTS
  ]);

  process.stdout.write(result.stdout ?? '');
  process.stderr.write(result.stderr ?? '');

  if ((result.status ?? 1) !== 0) {
    process.exit(result.status ?? 1);
  }

  enforceCoverageThresholds(result.stdout ?? '');
}

if (mode === 'critical') {
  runCriticalSuite();
} else {
  runCoverageSuite();
}
