const integrationTests = new Set([
  'attribution-e2e.integration.test.ts',
  'reporting-api.integration.test.ts',
  'tracking-request-context-fallback.integration.test.ts',
  'tracking-retention.integration.test.ts'
]);

const testArgs = ['tsx', '--test'];

if (mode === 'integration' || mode === 'all') {
  testArgs.push('--test-concurrency=1');
}

testArgs.push(...selectedTests);

const result = spawnSync('npx', testArgs, { ... });
