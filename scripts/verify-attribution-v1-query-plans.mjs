import pg from 'pg';

const { Client } = pg;

const REQUIRED_INDEXES = {
  touchpointSessionLookup: 'attribution_touchpoint_inputs_session_occurred_at_idx',
  summaryModelWindowLookup: 'attribution_model_summaries_model_order_time_idx',
  creditReportingLookup: 'attribution_model_credits_reporting_idx',
  explainRunOrderLookup: 'attribution_explain_records_run_order_stage_idx'
};

function requireDatabaseUrl() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required for attribution v1 query plan verification');
  }

  return process.env.DATABASE_URL;
}

function collectPlanNodes(node, names = []) {
  if (!node || typeof node !== 'object') {
    return names;
  }

  if (typeof node['Index Name'] === 'string') {
    names.push(node['Index Name']);
  }

  const plans = node.Plans;

  if (Array.isArray(plans)) {
    for (const plan of plans) {
      collectPlanNodes(plan, names);
    }
  }

  return names;
}

async function explainUsesIndex(client, label, sql, params, expectedIndex) {
  const result = await client.query(`EXPLAIN (FORMAT JSON) ${sql}`, params);
  const planRoot = result.rows[0]['QUERY PLAN'][0]?.Plan;
  const indexNames = collectPlanNodes(planRoot);

  if (!indexNames.includes(expectedIndex)) {
    throw new Error(
      `${label} did not use ${expectedIndex}. Planner used: ${indexNames.length > 0 ? indexNames.join(', ') : 'no index'}`
    );
  }

  process.stdout.write(`${label}: ${expectedIndex}\n`);
}

async function main() {
  const client = new Client({ connectionString: requireDatabaseUrl() });
  const verificationTag = `attr-v1-plan-${Date.now()}`;

  await client.connect();

  try {
    await client.query('BEGIN');

    await client.query(
      `
        INSERT INTO tracking_sessions (
          id,
          first_seen_at,
          last_seen_at,
          anonymous_user_id,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign,
          initial_gclid
        )
        SELECT
          gen_random_uuid(),
          now() - ((gs % 20) || ' days')::interval,
          now() - ((gs % 10) || ' minutes')::interval,
          $1 || '-anon-' || gs::text,
          'google',
          'cpc',
          'launch-' || (gs % 30)::text,
          'gclid-' || gs::text
        FROM generate_series(1, 4500) AS gs
      `,
      [verificationTag]
    );

    await client.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          shopify_order_number,
          currency_code,
          subtotal_price,
          total_price,
          processed_at,
          created_at_shopify,
          landing_session_id,
          checkout_token,
          cart_token,
          source_name,
          raw_payload,
          payload_source,
          payload_received_at,
          payload_external_id,
          payload_size_bytes,
          payload_hash
        )
        SELECT
          $1 || '-order-' || session_idx::text,
          $1 || '-order-' || session_idx::text,
          'USD',
          100.00,
          125.00,
          now() - ((session_idx % 25) || ' days')::interval,
          now() - ((session_idx % 25) || ' days')::interval,
          s.id,
          $1 || '-checkout-' || session_idx::text,
          $1 || '-cart-' || session_idx::text,
          'web',
          jsonb_build_object('verification_tag', $1::text, 'session_idx', session_idx),
          'shopify_order',
          now() - ((session_idx % 25) || ' days')::interval,
          $1 || '-order-' || session_idx::text,
          octet_length(
            convert_to(jsonb_build_object('verification_tag', $1::text, 'session_idx', session_idx)::text, 'utf8')
          ),
          encode(
            digest(jsonb_build_object('verification_tag', $1::text, 'session_idx', session_idx)::text, 'sha256'),
            'hex'
          )
        FROM (
          SELECT id, row_number() OVER (ORDER BY id) AS session_idx
          FROM tracking_sessions
          WHERE anonymous_user_id LIKE $1 || '-anon-%'
          ORDER BY id
          LIMIT 3200
        ) AS s
      `,
      [verificationTag]
    );

    const runResult = await client.query(
      `
        INSERT INTO attribution_runs (
          run_status,
          trigger_source,
          started_at_utc,
          completed_at_utc,
          run_metadata
        )
        VALUES (
          'completed',
          'plan_verifier',
          now() - interval '5 minutes',
          now(),
          jsonb_build_object('verification_tag', $1::text)
        )
        RETURNING id::text AS id
      `,
      [verificationTag]
    );

    const runId = runResult.rows[0]?.id;

    if (!runId) {
      throw new Error('Unable to create attribution run for plan verification');
    }

    await client.query(
      `
        INSERT INTO attribution_order_inputs (
          run_id,
          order_id,
          order_occurred_at_utc,
          order_timestamp_source,
          currency_code,
          subtotal_amount,
          total_amount,
          landing_session_id,
          checkout_token,
          cart_token,
          source_name,
          raw_order_ref
        )
        SELECT
          $2::uuid,
          o.shopify_order_id,
          o.processed_at,
          'processed_at',
          o.currency_code,
          o.subtotal_price,
          o.total_price,
          o.landing_session_id,
          o.checkout_token,
          o.cart_token,
          o.source_name,
          jsonb_build_object('verification_tag', $1::text, 'shopify_order_id', o.shopify_order_id)
        FROM shopify_orders AS o
        WHERE o.shopify_order_id LIKE $1 || '-order-%'
      `,
      [verificationTag, runId]
    );

    await client.query(
      `
        INSERT INTO attribution_touchpoint_inputs (
          run_id,
          order_id,
          touchpoint_id,
          session_id,
          touchpoint_occurred_at_utc,
          touchpoint_captured_at_utc,
          touchpoint_source_kind,
          ingestion_source,
          source,
          medium,
          campaign,
          content,
          term,
          click_id_type,
          click_id_value,
          evidence_source,
          is_direct,
          engagement_type,
          is_synthetic,
          is_eligible,
          attribution_reason,
          attribution_hint
        )
        SELECT
          $2::uuid,
          o.order_id,
          o.order_id || '-tp-' || event_idx::text,
          o.landing_session_id,
          o.processed_at - ((event_idx * 2) || ' hours')::interval,
          o.processed_at - ((event_idx * 2) || ' hours')::interval,
          CASE WHEN event_idx = 4 THEN 'shopify_hint' ELSE 'session_event' END,
          CASE WHEN event_idx = 4 THEN 'shopify_marketing_hint' ELSE 'browser' END,
          CASE WHEN event_idx = 3 THEN 'direct' ELSE 'google' END,
          CASE WHEN event_idx = 3 THEN NULL ELSE 'cpc' END,
          'launch-' || (order_idx % 30)::text,
          'creative-' || event_idx::text,
          'term-' || (order_idx % 10)::text,
          CASE WHEN event_idx IN (1, 2) THEN 'gclid' ELSE NULL END,
          CASE WHEN event_idx IN (1, 2) THEN 'gclid-' || order_idx::text || '-' || event_idx::text ELSE NULL END,
          CASE
            WHEN event_idx = 4 THEN 'shopify_marketing_hint'
            WHEN event_idx = 2 THEN 'checkout_token'
            ELSE 'landing_session_id'
          END,
          event_idx = 3,
          CASE WHEN event_idx = 4 THEN 'view' ELSE 'click' END,
          event_idx = 4,
          true,
          CASE WHEN event_idx = 4 THEN 'qualifying_shopify_hint' ELSE 'eligible_touch' END,
          jsonb_build_object('verification_tag', $1::text, 'event_idx', event_idx)
        FROM (
          SELECT
            order_id,
            landing_session_id,
            order_occurred_at_utc AS processed_at,
            row_number() OVER (ORDER BY order_id) AS order_idx
          FROM attribution_order_inputs
          WHERE run_id = $2::uuid
          ORDER BY order_id
          LIMIT 3200
        ) AS o
        CROSS JOIN generate_series(1, 4) AS event_idx
      `,
      [verificationTag, runId]
    );

    await client.query(
      `
        INSERT INTO attribution_model_summaries (
          run_id,
          order_id,
          model_key,
          allocation_status,
          winner_touchpoint_id,
          winner_session_id,
          winner_evidence_source,
          winner_attribution_reason,
          total_credit_weight,
          total_revenue_credited,
          touchpoint_count_considered,
          eligible_click_count,
          eligible_view_count,
          lookback_rule_applied,
          winner_selection_rule,
          direct_suppression_applied,
          deterministic_block_applied,
          normalization_failures_count,
          order_occurred_at_utc
        )
        SELECT
          $1::uuid,
          o.order_id,
          'last_non_direct',
          'attributed',
          o.order_id || '-tp-2',
          o.landing_session_id,
          'checkout_token',
          'latest_non_direct_touch',
          1.0,
          o.total_amount,
          4,
          3,
          1,
          'mixed',
          'last_non_direct',
          true,
          false,
          0,
          o.order_occurred_at_utc
        FROM attribution_order_inputs AS o
        WHERE o.run_id = $1::uuid
      `,
      [runId]
    );

    await client.query(
      `
        INSERT INTO attribution_model_credits (
          run_id,
          order_id,
          model_key,
          touchpoint_id,
          session_id,
          touchpoint_position,
          occurred_at_utc,
          source,
          medium,
          campaign,
          content,
          term,
          click_id_type,
          click_id_value,
          touch_type,
          is_direct,
          evidence_source,
          is_synthetic,
          attribution_reason,
          credit_weight,
          revenue_credit,
          is_primary,
          match_source,
          confidence_label
        )
        SELECT
          t.run_id,
          t.order_id,
          'last_non_direct',
          t.touchpoint_id,
          t.session_id,
          1,
          t.touchpoint_occurred_at_utc,
          t.source,
          t.medium,
          t.campaign,
          t.content,
          t.term,
          t.click_id_type,
          t.click_id_value,
          CASE WHEN t.engagement_type = 'view' THEN 'view' ELSE 'click' END,
          t.is_direct,
          t.evidence_source,
          t.is_synthetic,
          'latest_non_direct_touch',
          1.0,
          o.total_amount,
          true,
          t.evidence_source,
          CASE WHEN t.is_synthetic THEN 'low' ELSE 'high' END
        FROM attribution_touchpoint_inputs AS t
        INNER JOIN attribution_order_inputs AS o
          ON o.run_id = t.run_id
         AND o.order_id = t.order_id
        WHERE t.run_id = $1::uuid
          AND t.touchpoint_id LIKE '%-tp-2'
      `,
      [runId]
    );

    await client.query(
      `
        INSERT INTO attribution_explain_records (
          run_id,
          order_id,
          touchpoint_id,
          model_key,
          explain_stage,
          decision,
          decision_reason,
          details_json,
          order_occurred_at_utc
        )
        SELECT
          t.run_id,
          t.order_id,
          t.touchpoint_id,
          'last_non_direct',
          'model_scoring',
          CASE WHEN t.touchpoint_id LIKE '%-tp-2' THEN 'winner' ELSE 'no_credit' END,
          CASE WHEN t.touchpoint_id LIKE '%-tp-2' THEN 'latest_non_direct_touch' ELSE 'not_selected' END,
          jsonb_build_object('verification_tag', $1::text, 'touchpoint_id', t.touchpoint_id),
          o.order_occurred_at_utc
        FROM attribution_touchpoint_inputs AS t
        INNER JOIN attribution_order_inputs AS o
          ON o.run_id = t.run_id
         AND o.order_id = t.order_id
        WHERE t.run_id = $2::uuid
      `,
      [verificationTag, runId]
    );

    await client.query('ANALYZE tracking_sessions');
    await client.query('ANALYZE shopify_orders');
    await client.query('ANALYZE attribution_runs');
    await client.query('ANALYZE attribution_order_inputs');
    await client.query('ANALYZE attribution_touchpoint_inputs');
    await client.query('ANALYZE attribution_model_summaries');
    await client.query('ANALYZE attribution_model_credits');
    await client.query('ANALYZE attribution_explain_records');

    const sessionLookupResult = await client.query(
      `
        SELECT session_id::text
        FROM attribution_touchpoint_inputs
        WHERE run_id = $1::uuid
          AND session_id IS NOT NULL
        ORDER BY touchpoint_occurred_at_utc DESC
        LIMIT 1
      `,
      [runId]
    );

    const sessionId = sessionLookupResult.rows[0]?.session_id;

    if (!sessionId) {
      throw new Error('Unable to select a touchpoint session for plan verification');
    }

    const orderLookupResult = await client.query(
      `
        SELECT order_id
        FROM attribution_order_inputs
        WHERE run_id = $1::uuid
        ORDER BY order_occurred_at_utc DESC, order_id ASC
        LIMIT 1
      `,
      [runId]
    );

    const orderId = orderLookupResult.rows[0]?.order_id;

    if (!orderId) {
      throw new Error('Unable to select an order for plan verification');
    }

    await explainUsesIndex(
      client,
      'touchpoint_session_lookup',
      `
        SELECT id
        FROM attribution_touchpoint_inputs
        WHERE session_id = $1::uuid
        ORDER BY touchpoint_occurred_at_utc DESC
        LIMIT 20
      `,
      [sessionId],
      REQUIRED_INDEXES.touchpointSessionLookup
    );

    await explainUsesIndex(
      client,
      'summary_model_window_lookup',
      `
        SELECT id
        FROM attribution_model_summaries
        WHERE model_key = $1
          AND order_occurred_at_utc >= now() - interval '30 days'
        ORDER BY order_occurred_at_utc DESC
        LIMIT 50
      `,
      ['last_non_direct'],
      REQUIRED_INDEXES.summaryModelWindowLookup
    );

    await explainUsesIndex(
      client,
      'credit_reporting_lookup',
      `
        SELECT id
        FROM attribution_model_credits
        WHERE model_key = $1
          AND source = $2
          AND medium = $3
          AND campaign = $4
        ORDER BY occurred_at_utc DESC
        LIMIT 50
      `,
      ['last_non_direct', 'google', 'cpc', 'launch-1'],
      REQUIRED_INDEXES.creditReportingLookup
    );

    await explainUsesIndex(
      client,
      'explain_run_order_lookup',
      `
        SELECT id
        FROM attribution_explain_records
        WHERE run_id = $1::uuid
          AND order_id = $2
          AND explain_stage = $3
        ORDER BY created_at_utc DESC
        LIMIT 50
      `,
      [runId, orderId, 'model_scoring'],
      REQUIRED_INDEXES.explainRunOrderLookup
    );

    await client.query('ROLLBACK');
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exit(1);
});
