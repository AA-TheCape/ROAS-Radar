import pg from 'pg';

const { Client } = pg;

const REQUIRED_INDEXES = {
  session_lookup: 'session_attribution_touch_events_session_occurred_at_idx',
  order_lookup: 'order_attribution_links_order_lookup_idx',
  event_timestamp_lookup: 'session_attribution_touch_events_occurred_at_idx'
};

function requireDatabaseUrl() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required for attribution query plan verification');
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
  const verificationTag = `plan-${Date.now()}`;

  await client.connect();

  try {
    await client.query('BEGIN');

    await client.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          shopify_order_number,
          currency_code,
          subtotal_price,
          total_price,
          raw_payload
        )
        SELECT
          $1 || '-order-' || gs::text,
          $1 || '-order-' || gs::text,
          'USD',
          100.00,
          125.00,
          '{}'::jsonb
        FROM generate_series(1, 3000) AS gs
        ON CONFLICT (shopify_order_id) DO NOTHING
      `,
      [verificationTag]
    );

    await client.query(
      `
        INSERT INTO session_attribution_identities (
          roas_radar_session_id,
          first_captured_at,
          last_captured_at,
          retained_until,
          landing_url,
          referrer_url,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign,
          initial_gclid
        )
        SELECT
          gen_random_uuid(),
          now() - (gs || ' minutes')::interval,
          now() - ((gs - 1) || ' minutes')::interval,
          now() + interval '30 days',
          'https://store.example.com/?utm_source=google&utm_medium=cpc&utm_campaign=launch-' || gs::text,
          'https://www.google.com/',
          'google',
          'cpc',
          'launch-' || (gs % 20)::text,
          'gclid-' || gs::text
        FROM generate_series(1, 4000) AS gs
      `
    );

    await client.query(
      `
        INSERT INTO session_attribution_touch_events (
          roas_radar_session_id,
          event_type,
          occurred_at,
          captured_at,
          retained_until,
          page_url,
          referrer_url,
          utm_source,
          utm_medium,
          utm_campaign,
          gclid,
          ingestion_source,
          raw_payload
        )
        SELECT
          s.roas_radar_session_id,
          CASE WHEN event_idx % 5 = 0 THEN 'checkout_started' ELSE 'page_view' END,
          now() - ((session_idx * 5 + event_idx) || ' minutes')::interval,
          now() - ((session_idx * 5 + event_idx) || ' minutes')::interval,
          now() + interval '30 days',
          'https://store.example.com/products/' || session_idx::text || '?event=' || event_idx::text,
          'https://www.google.com/',
          'google',
          'cpc',
          'launch-' || (session_idx % 20)::text,
          'gclid-' || session_idx::text,
          CASE WHEN event_idx % 2 = 0 THEN 'browser' ELSE 'server' END,
          jsonb_build_object('session_idx', session_idx, 'event_idx', event_idx)
        FROM (
          SELECT roas_radar_session_id, row_number() OVER (ORDER BY roas_radar_session_id) AS session_idx
          FROM session_attribution_identities
          ORDER BY roas_radar_session_id
          LIMIT 4000
        ) AS s
        CROSS JOIN generate_series(1, 40) AS event_idx
      `
    );

    await client.query(
      `
        INSERT INTO order_attribution_links (
          shopify_order_id,
          roas_radar_session_id,
          attribution_model,
          link_type,
          attribution_reason,
          linked_at,
          order_occurred_at,
          is_primary,
          retained_until,
          created_at
        )
        SELECT
          $1 || '-order-' || session_idx::text,
          s.roas_radar_session_id,
          'last_non_direct',
          CASE WHEN session_idx % 3 = 0 THEN 'deterministic' ELSE 'eligible' END,
          'query_plan_verification',
          now() - (session_idx || ' minutes')::interval,
          now() - (session_idx || ' minutes')::interval,
          session_idx % 3 = 0,
          now() + interval '30 days',
          now()
        FROM (
          SELECT roas_radar_session_id, row_number() OVER (ORDER BY roas_radar_session_id) AS session_idx
          FROM session_attribution_identities
          ORDER BY roas_radar_session_id
          LIMIT 3000
        ) AS s
      `,
      [verificationTag]
    );

    await client.query('ANALYZE session_attribution_identities');
    await client.query('ANALYZE session_attribution_touch_events');
    await client.query('ANALYZE order_attribution_links');

    const sessionLookupResult = await client.query(
      `
        SELECT roas_radar_session_id::text
        FROM session_attribution_identities
        ORDER BY last_captured_at DESC, roas_radar_session_id ASC
        LIMIT 1
      `
    );

    const sessionId = sessionLookupResult.rows[0]?.roas_radar_session_id;

    if (!sessionId) {
      throw new Error('Unable to select a session for plan verification');
    }

    await explainUsesIndex(
      client,
      'session_lookup',
      `
        SELECT id
        FROM session_attribution_touch_events
        WHERE roas_radar_session_id = $1::uuid
        ORDER BY occurred_at DESC
        LIMIT 20
      `,
      [sessionId],
      REQUIRED_INDEXES.session_lookup
    );

    await explainUsesIndex(
      client,
      'order_lookup',
      `
        SELECT id
        FROM order_attribution_links
        WHERE shopify_order_id = $1
        ORDER BY is_primary DESC, linked_at DESC
      `,
      [`${verificationTag}-order-3`],
      REQUIRED_INDEXES.order_lookup
    );

    await explainUsesIndex(
      client,
      'event_timestamp_lookup',
      `
        SELECT id
        FROM session_attribution_touch_events
        WHERE occurred_at >= now() - interval '6 hours'
        ORDER BY occurred_at DESC
        LIMIT 500
      `,
      [],
      REQUIRED_INDEXES.event_timestamp_lookup
    );

    await client.query('ROLLBACK');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exit(1);
});
