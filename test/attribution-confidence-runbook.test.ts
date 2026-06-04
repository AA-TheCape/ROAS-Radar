import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.join(__dirname, '..');
const runbookText = readFileSync(path.join(repoRoot, 'docs', 'runbooks', 'attribution-confidence-scoring.md'), 'utf8');
const packageJson = JSON.parse(readFileSync(path.join(repoRoot, 'package.json'), 'utf8')) as {
  scripts: Record<string, string>;
};

test('confidence scoring runbook uses the implemented confidence backfill command', () => {
  assert.equal(packageJson.scripts['attribution:backfill-confidence'], 'tsx src/admin/backfill-attribution-confidence.ts');
  assert.match(runbookText, /npm run attribution:backfill-confidence -- --dry-run --batch-size 1000/);
  assert.match(
    runbookText,
    /npm run attribution:backfill-confidence -- --batch-size 1000 --resume-after-order-row-id 123456/
  );
  assert.doesNotMatch(runbookText, /attribution:backfill-confidence:start/);
});

test('confidence scoring runbook documents current attribution admin and read API paths', () => {
  assert.match(runbookText, /\$API_BASE_URL\/api\/reporting\/orders\?/);
  assert.match(runbookText, /\$API_BASE_URL\/api\/reporting\/orders\/6123456789012/);
  assert.match(runbookText, /\$API_BASE_URL\/api\/attribution\/results\?/);
  assert.match(runbookText, /\$API_BASE_URL\/api\/attribution\/orders\/6123456789012\/explainability\?/);
  assert.match(runbookText, /\$API_BASE_URL\/api\/admin\/attribution\/orders\/backfill"/);
  assert.match(runbookText, /\$API_BASE_URL\/api\/admin\/attribution\/orders\/backfill\/\$JOB_ID/);
});

test('confidence scoring runbook SQL examples use current confidence metadata schema', () => {
  for (const tableName of [
    'attribution_sources',
    'matching_methods',
    'shopify_orders',
    'attribution_results',
    'order_attribution_backfill_runs'
  ]) {
    assert.match(runbookText, new RegExp(`\\b${tableName}\\b`));
  }

  for (const columnName of [
    'attribution_source_id',
    'matching_method_id',
    'attribution_confidence_score',
    'attribution_confidence_contract_version',
    'confidence_score',
    'confidence_contract_version',
    'last_attribution_run_at',
    'last_heartbeat_at'
  ]) {
    assert.match(runbookText, new RegExp(`\\b${columnName}\\b`));
  }
});
