import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);

async function requestJson(
  server: ReturnType<typeof createServer>,
  path: string,
  headers: Record<string, string> = {
    authorization: 'Bearer test-reporting-token'
  }
) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    headers
  });
  const body = await response.json();

  return { response, body };
}

test('attribution read routes require authentication', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/attribution/results?modelKey=last_non_direct&startDate=2026-04-01&endDate=2026-04-10',
      {}
    );

    assert.equal(response.status, 401);
    assert.deepEqual(body, {
      error: 'unauthorized',
      message: 'Authentication required'
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('attribution results return paginated order-model summaries with run and primary-touch metadata', async () => {
  const queries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    queries.push({ text, params });

    if (text.includes('INSERT INTO app_settings')) {
      return { rows: [] };
    }

    if (text.includes('SELECT reporting_timezone, updated_at') && text.includes('FROM app_settings')) {
      return {
        rowCount: 1,
        rows: [
          {
            reporting_timezone: 'America/Los_Angeles',
            updated_at: new Date('2026-04-01T00:00:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM app_settings')) {
      return { rows: [] };
    }

    assert.match(text, /FROM attribution_model_summaries summary/);
    assert.equal(params?.[0], 'last_non_direct');
    assert.equal(params?.[1], '2026-04-01');
    assert.equal(params?.[2], '2026-04-10');
    assert.equal(params?.[4], 'google');
    assert.equal(params?.[5], 'cpc');
    assert.equal(params?.[6], 'spring-sale');
    assert.equal(params?.[7], 3);

    return {
      rows: [
        {
          run_id: '11111111-1111-4111-8111-111111111111',
          attribution_spec_version: 'v1',
          order_id: 'order-2',
          model_key: 'last_non_direct',
          allocation_status: 'attributed',
          winner_touchpoint_id: 'tp-2',
          winner_session_id: '22222222-2222-4222-8222-222222222222',
          winner_evidence_source: 'landing_session_id',
          winner_attribution_reason: 'matched_by_landing_session',
          total_credit_weight: '1.00000000',
          total_revenue_credited: '88.50',
          touchpoint_count_considered: 2,
          eligible_click_count: 2,
          eligible_view_count: 0,
          lookback_rule_applied: '28d_click',
          winner_selection_rule: 'last_non_direct',
          direct_suppression_applied: true,
          deterministic_block_applied: false,
          normalization_failures_count: 0,
          generated_at_utc: new Date('2026-04-10T12:05:00.000Z'),
          order_occurred_at_utc: new Date('2026-04-10T12:00:00.000Z'),
          run_status: 'completed',
          trigger_source: 'manual',
          submitted_by: 'admin@example.com',
          window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
          window_end_utc: new Date('2026-04-10T23:59:59.000Z'),
          lookback_click_window_days: 28,
          lookback_view_window_days: 7,
          run_created_at_utc: new Date('2026-04-10T12:04:00.000Z'),
          completed_at_utc: new Date('2026-04-10T12:06:00.000Z'),
          primary_touchpoint_id: 'tp-2',
          primary_session_id: '22222222-2222-4222-8222-222222222222',
          primary_occurred_at_utc: new Date('2026-04-09T11:00:00.000Z'),
          primary_source: 'google',
          primary_medium: 'cpc',
          primary_campaign: 'spring-sale',
          primary_content: 'hero',
          primary_term: 'widget',
          primary_click_id_type: 'gclid',
          primary_click_id_value: 'ABC123',
          primary_touch_type: 'click',
          primary_is_direct: false,
          primary_is_synthetic: false,
          primary_attribution_reason: 'matched_by_landing_session'
        },
        {
          run_id: '11111111-1111-4111-8111-111111111110',
          attribution_spec_version: 'v1',
          order_id: 'order-1',
          model_key: 'last_non_direct',
          allocation_status: 'no_eligible_touches',
          winner_touchpoint_id: null,
          winner_session_id: null,
          winner_evidence_source: null,
          winner_attribution_reason: null,
          total_credit_weight: '0.00000000',
          total_revenue_credited: '0.00',
          touchpoint_count_considered: 0,
          eligible_click_count: 0,
          eligible_view_count: 0,
          lookback_rule_applied: '28d_click',
          winner_selection_rule: 'last_non_direct',
          direct_suppression_applied: false,
          deterministic_block_applied: false,
          normalization_failures_count: 1,
          generated_at_utc: new Date('2026-04-09T12:05:00.000Z'),
          order_occurred_at_utc: new Date('2026-04-09T12:00:00.000Z'),
          run_status: 'completed',
          trigger_source: 'manual',
          submitted_by: 'admin@example.com',
          window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
          window_end_utc: new Date('2026-04-10T23:59:59.000Z'),
          lookback_click_window_days: 28,
          lookback_view_window_days: 7,
          run_created_at_utc: new Date('2026-04-09T12:04:00.000Z'),
          completed_at_utc: new Date('2026-04-09T12:06:00.000Z'),
          primary_touchpoint_id: null,
          primary_session_id: null,
          primary_occurred_at_utc: null,
          primary_source: null,
          primary_medium: null,
          primary_campaign: null,
          primary_content: null,
          primary_term: null,
          primary_click_id_type: null,
          primary_click_id_value: null,
          primary_touch_type: null,
          primary_is_direct: null,
          primary_is_synthetic: null,
          primary_attribution_reason: null
        },
        {
          run_id: '11111111-1111-4111-8111-111111111109',
          attribution_spec_version: 'v1',
          order_id: 'order-0',
          model_key: 'last_non_direct',
          allocation_status: 'attributed',
          winner_touchpoint_id: 'tp-0',
          winner_session_id: '22222222-2222-4222-8222-222222222220',
          winner_evidence_source: 'landing_session_id',
          winner_attribution_reason: 'matched_by_landing_session',
          total_credit_weight: '1.00000000',
          total_revenue_credited: '42.00',
          touchpoint_count_considered: 1,
          eligible_click_count: 1,
          eligible_view_count: 0,
          lookback_rule_applied: '28d_click',
          winner_selection_rule: 'last_non_direct',
          direct_suppression_applied: false,
          deterministic_block_applied: false,
          normalization_failures_count: 0,
          generated_at_utc: new Date('2026-04-08T12:05:00.000Z'),
          order_occurred_at_utc: new Date('2026-04-08T12:00:00.000Z'),
          run_status: 'completed',
          trigger_source: 'manual',
          submitted_by: 'admin@example.com',
          window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
          window_end_utc: new Date('2026-04-10T23:59:59.000Z'),
          lookback_click_window_days: 28,
          lookback_view_window_days: 7,
          run_created_at_utc: new Date('2026-04-08T12:04:00.000Z'),
          completed_at_utc: new Date('2026-04-08T12:06:00.000Z'),
          primary_touchpoint_id: 'tp-0',
          primary_session_id: '22222222-2222-4222-8222-222222222220',
          primary_occurred_at_utc: new Date('2026-04-07T11:00:00.000Z'),
          primary_source: 'google',
          primary_medium: 'cpc',
          primary_campaign: 'spring-sale',
          primary_content: 'retargeting',
          primary_term: 'widget',
          primary_click_id_type: 'gclid',
          primary_click_id_value: 'DEF456',
          primary_touch_type: 'click',
          primary_is_direct: false,
          primary_is_synthetic: false,
          primary_attribution_reason: 'matched_by_landing_session'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/attribution/results?modelKey=last_non_direct&startDate=2026-04-01&endDate=2026-04-10&source=google&medium=cpc&campaign=spring-sale&limit=2'
    );

    assert.equal(response.status, 200);
    assert.equal(body.rows.length, 2);
    assert.equal(body.rows[0].record.run_id, '11111111-1111-4111-8111-111111111111');
    assert.equal(body.rows[0].run.lookbackClickWindowDays, 28);
    assert.equal(body.rows[0].model.key, 'last_non_direct');
    assert.equal(body.rows[0].primaryTouchpoint.touchpoint_id, 'tp-2');
    assert.equal(body.rows[1].primaryTouchpoint, null);
    assert.equal(typeof body.nextCursor, 'string');
    assert.equal(queries.filter((entry) => entry.text.includes('app_settings')).length, 2);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('attribution channel totals return side-by-side model aggregates by credited channel', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('INSERT INTO app_settings')) {
      return { rows: [] };
    }

    if (text.includes('SELECT reporting_timezone, updated_at') && text.includes('FROM app_settings')) {
      return {
        rowCount: 1,
        rows: [
          {
            reporting_timezone: 'America/Los_Angeles',
            updated_at: new Date('2026-04-01T00:00:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM app_settings')) {
      return { rows: [] };
    }

    assert.match(text, /FROM attribution_model_credits credit/);
    assert.equal(params?.[0], '2026-04-01');
    assert.equal(params?.[1], '2026-04-10');
    assert.equal(params?.[3], 'google');
    assert.equal(params?.[4], 'cpc');
    assert.equal(params?.[5], 'spring-sale');

    return {
      rows: [
        {
          model_key: 'last_non_direct',
          source: 'google',
          medium: 'cpc',
          order_count: 3,
          revenue_credited: '260.50',
          credit_weight_total: '3.00000000',
          lookback_click_window_days: 28,
          lookback_view_window_days: 7
        },
        {
          model_key: 'linear',
          source: 'google',
          medium: 'cpc',
          order_count: 4,
          revenue_credited: '180.25',
          credit_weight_total: '1.75000000',
          lookback_click_window_days: 28,
          lookback_view_window_days: 7
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/attribution/channel-totals?startDate=2026-04-01&endDate=2026-04-10&source=google&medium=cpc&campaign=spring-sale'
    );

    assert.equal(response.status, 200);
    assert.equal(body.lookbackClickWindowDays, 28);
    assert.equal(body.lookbackViewWindowDays, 7);
    assert.deepEqual(body.rows, [
      {
        modelKey: 'last_non_direct',
        source: 'google',
        medium: 'cpc',
        orderCount: 3,
        revenueCredited: '260.50',
        creditWeightTotal: '3.00000000'
      },
      {
        modelKey: 'linear',
        source: 'google',
        medium: 'cpc',
        orderCount: 4,
        revenueCredited: '180.25',
        creditWeightTotal: '1.75000000'
      }
    ]);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('attribution explainability returns touchpoints, model summaries, credits, and explain records for one order', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('SELECT summary.run_id::text AS run_id') && text.includes('LIMIT 1')) {
      return {
        rows: [{ run_id: '11111111-1111-4111-8111-111111111111' }]
      };
    }

    if (text.includes('FROM attribution_runs')) {
      return {
        rows: [
          {
            id: '11111111-1111-4111-8111-111111111111',
            attribution_spec_version: 'v1',
            run_status: 'completed',
            trigger_source: 'manual',
            submitted_by: 'internal',
            window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
            window_end_utc: new Date('2026-04-30T23:59:59.000Z'),
            lookback_click_window_days: 28,
            lookback_view_window_days: 7,
            created_at_utc: new Date('2026-04-30T12:00:00.000Z'),
            completed_at_utc: new Date('2026-04-30T12:05:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM attribution_model_summaries summary')) {
      return {
        rows: [
          {
            run_id: '11111111-1111-4111-8111-111111111111',
            attribution_spec_version: 'v1',
            order_id: 'order-1',
            model_key: 'last_non_direct',
            allocation_status: 'attributed',
            winner_touchpoint_id: 'tp-1',
            winner_session_id: '22222222-2222-4222-8222-222222222222',
            winner_evidence_source: 'landing_session_id',
            winner_attribution_reason: 'matched_by_landing_session',
            total_credit_weight: '1.00000000',
            total_revenue_credited: '120.00',
            touchpoint_count_considered: 2,
            eligible_click_count: 2,
            eligible_view_count: 0,
            lookback_rule_applied: '28d_click',
            winner_selection_rule: 'last_non_direct',
            direct_suppression_applied: true,
            deterministic_block_applied: false,
            normalization_failures_count: 0,
            generated_at_utc: new Date('2026-04-30T12:05:00.000Z'),
            order_occurred_at_utc: new Date('2026-04-30T12:00:00.000Z'),
            run_status: 'completed',
            trigger_source: 'manual',
            submitted_by: 'internal',
            window_start_utc: new Date('2026-04-01T00:00:00.000Z'),
            window_end_utc: new Date('2026-04-30T23:59:59.000Z'),
            lookback_click_window_days: 28,
            lookback_view_window_days: 7,
            run_created_at_utc: new Date('2026-04-30T12:00:00.000Z'),
            completed_at_utc: new Date('2026-04-30T12:05:00.000Z'),
            primary_touchpoint_id: null,
            primary_session_id: null,
            primary_occurred_at_utc: null,
            primary_source: null,
            primary_medium: null,
            primary_campaign: null,
            primary_content: null,
            primary_term: null,
            primary_click_id_type: null,
            primary_click_id_value: null,
            primary_touch_type: null,
            primary_is_direct: null,
            primary_is_synthetic: null,
            primary_attribution_reason: null
          }
        ]
      };
    }

    if (text.includes('FROM attribution_touchpoint_inputs')) {
      return {
        rows: [
          {
            run_id: '11111111-1111-4111-8111-111111111111',
            order_id: 'order-1',
            touchpoint_id: 'tp-1',
            session_id: '22222222-2222-4222-8222-222222222222',
            identity_journey_id: null,
            touchpoint_occurred_at_utc: new Date('2026-04-28T10:00:00.000Z'),
            touchpoint_captured_at_utc: new Date('2026-04-28T10:00:01.000Z'),
            touchpoint_source_kind: 'session_event',
            ingestion_source: 'browser',
            source: 'google',
            medium: 'cpc',
            campaign: 'spring-sale',
            content: 'hero',
            term: 'widget',
            click_id_type: 'gclid',
            click_id_value: 'ABC123',
            evidence_source: 'landing_session_id',
            is_direct: false,
            engagement_type: 'click',
            is_synthetic: false,
            is_eligible: true,
            ineligibility_reason: null,
            attribution_reason: 'matched_by_landing_session',
            attribution_hint: {}
          }
        ]
      };
    }

    if (text.includes('FROM attribution_model_credits')) {
      assert.equal(params?.[0], '11111111-1111-4111-8111-111111111111');
      return {
        rows: [
          {
            run_id: '11111111-1111-4111-8111-111111111111',
            attribution_spec_version: 'v1',
            order_id: 'order-1',
            model_key: 'last_non_direct',
            touchpoint_id: 'tp-1',
            session_id: '22222222-2222-4222-8222-222222222222',
            touchpoint_position: 1,
            occurred_at_utc: new Date('2026-04-28T10:00:00.000Z'),
            source: 'google',
            medium: 'cpc',
            campaign: 'spring-sale',
            content: 'hero',
            term: 'widget',
            click_id_type: 'gclid',
            click_id_value: 'ABC123',
            touch_type: 'click',
            is_direct: false,
            evidence_source: 'landing_session_id',
            is_synthetic: false,
            attribution_reason: 'matched_by_landing_session',
            credit_weight: '1.00000000',
            revenue_credit: '120.00',
            is_primary: true
          }
        ]
      };
    }

    if (text.includes('FROM attribution_explain_records')) {
      return {
        rows: [
          {
            run_id: '11111111-1111-4111-8111-111111111111',
            order_id: 'order-1',
            touchpoint_id: 'tp-1',
            model_key: 'last_non_direct',
            explain_stage: 'model_scoring',
            decision: 'winner',
            decision_reason: 'matched_by_landing_session',
            details_json: { creditWeight: 1 },
            order_occurred_at_utc: new Date('2026-04-30T12:00:00.000Z'),
            created_at_utc: new Date('2026-04-30T12:05:01.000Z')
          }
        ]
      };
    }

    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/attribution/orders/order-1/explainability');

    assert.equal(response.status, 200);
    assert.equal(body.orderId, 'order-1');
    assert.equal(body.selectedRunReason, 'latest_run_for_order');
    assert.equal(body.run.id, '11111111-1111-4111-8111-111111111111');
    assert.equal(body.summaries[0].model_key, 'last_non_direct');
    assert.equal(body.touchpoints[0].touchpointId, 'tp-1');
    assert.equal(body.credits[0].touchpoint_id, 'tp-1');
    assert.equal(body.explainability[0].decision, 'winner');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
