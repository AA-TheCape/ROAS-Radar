import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

const EXPECTED_INDEXES = [
  'attribution_runs_status_created_idx',
  'attribution_order_inputs_run_order_time_idx',
  'attribution_order_inputs_landing_session_idx',
  'attribution_touchpoint_inputs_session_occurred_at_idx',
  'attribution_touchpoint_inputs_run_order_eligibility_idx',
  'attribution_model_summaries_model_order_time_idx',
  'attribution_model_summaries_run_model_status_idx',
  'attribution_model_credits_reporting_idx',
  'attribution_model_credits_order_model_idx',
  'attribution_explain_records_run_order_stage_idx'
];

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('attribution v1 indexes are present after migrations', async () => {
  const indexResult = await pool.query<{ indexname: string }>(
    `
      SELECT indexname
      FROM pg_indexes
      WHERE schemaname = 'public'
        AND indexname = ANY($1::text[])
      ORDER BY indexname ASC
    `,
    [EXPECTED_INDEXES]
  );

  assert.deepEqual(
    indexResult.rows.map((row) => row.indexname),
    [...EXPECTED_INDEXES].sort()
  );
});
