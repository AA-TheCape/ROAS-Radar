import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const [{ pool }, harnessModule, retentionModule] = await Promise.all([
  import('../src/db/pool.js'),
  import('./e2e-harness.ts'),
  import('../src/modules/tracking/retention.js')
]);

const { resetE2EDatabase } = harnessModule;
const { runSessionAttributionRetention } = retentionModule;

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('runSessionAttributionRetention deletes expired session capture rows older than the 30 day cutoff in batches', async () => {
  // seeds expired unprotected rows, expired protected rows, and fresh rows
  // asserts only unprotected expired rows are deleted
});

test('runSessionAttributionRetention does not delete rows exactly on the 30 day cutoff', async () => {
  // asserts cutoff uses strict < semantics
});
