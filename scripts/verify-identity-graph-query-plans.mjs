import pg from 'pg';

const { Client } = pg;

const REQUIRED_INDEXES = {
  journey_lookup: 'identity_journeys_authoritative_shopify_customer_uidx',
  node_lookup: 'identity_nodes_hashed_email_lookup_idx',
  active_edge_lookup: 'identity_edges_active_node_uidx',
  journey_edge_lookup: 'identity_edges_journey_active_rank_idx',
  lookback_lookup: 'identity_journeys_lookback_expires_at_idx'
};

function requireDatabaseUrl() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required for identity graph query plan verification');
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
  const verificationTag = `identity-plan-${Date.now()}`;

  await client.connect();

  try {
    await client.query('BEGIN');

    await client.query(
      `
        INSERT INTO identity_journeys (
          authoritative_shopify_customer_id,
          primary_email_hash,
          status,
          merge_version,
          lookback_window_started_at,
          lookback_window_expires_at,
          last_touch_eligible_at,
          first_source_system,
          first_source_table,
          first_source_record_id,
          last_source_system,
          last_source_table,
          last_source_record_id
        )
        SELECT
          $1 || '-customer-' || gs::text,
          lpad(to_hex(gs), 64, '0'),
          'active',
          1,
          now() - ((gs % 10) || ' days')::interval,
          now() + ((gs % 20) || ' days')::interval,
          now() - ((gs % 5) || ' days')::interval,
          'plan_verifier',
          'synthetic_identity_graph',
          $1 || '-journey-' || gs::text,
          'plan_verifier',
          'synthetic_identity_graph',
          $1 || '-journey-' || gs::text
        FROM generate_series(1, 2500) AS gs
      `,
      [verificationTag]
    );

    await client.query(
      `
        INSERT INTO identity_nodes (
          node_type,
          node_key,
          is_authoritative,
          is_ambiguous,
          first_seen_at,
          last_seen_at
        )
        SELECT
          'hashed_email',
          $1 || '-email-' || gs::text,
          false,
          false,
          now() - ((gs % 30) || ' days')::interval,
          now() - ((gs % 2) || ' hours')::interval
        FROM generate_series(1, 3000) AS gs
      `,
      [verificationTag]
    );

    await client.query(
      `
        INSERT INTO identity_nodes (
          node_type,
          node_key,
          is_authoritative,
          is_ambiguous,
          first_seen_at,
          last_seen_at
        )
        SELECT
          'session_id',
          $1 || '-session-' || gs::text,
          false,
          false,
          now() - ((gs % 30) || ' days')::interval,
          now()
        FROM generate_series(1, 3000) AS gs
      `,
      [verificationTag]
    );

    await client.query(
      `
        INSERT INTO identity_edges (
          node_id,
          journey_id,
          edge_type,
          precedence_rank,
          evidence_source,
          source_table,
          source_record_id,
          is_active,
          first_observed_at,
          last_observed_at
        )
        SELECT
          nodes.id,
          journeys.id,
          'deterministic',
          CASE
            WHEN nodes.node_type = 'hashed_email' THEN 70
            ELSE 20
          END,
          'plan_verifier',
          'synthetic_identity_graph',
          nodes.node_key,
          true,
          now() - interval '2 days',
          now() - ((row_number() OVER (ORDER BY nodes.node_key) % 10) || ' minutes')::interval
        FROM (
          SELECT id, node_type, node_key, row_number() OVER (ORDER BY node_key) AS row_num
          FROM identity_nodes
          WHERE node_key LIKE $1 || '-%'
        ) AS nodes
        INNER JOIN (
          SELECT id, row_number() OVER (ORDER BY authoritative_shopify_customer_id) AS row_num
          FROM identity_journeys
          WHERE authoritative_shopify_customer_id LIKE $1 || '-customer-%'
        ) AS journeys
          ON journeys.row_num = ((nodes.row_num - 1) % 2500) + 1
      `,
      [verificationTag]
    );

    await client.query('ANALYZE identity_journeys');
    await client.query('ANALYZE identity_nodes');
    await client.query('ANALYZE identity_edges');

    const journeyLookupResult = await client.query(
      `
        SELECT authoritative_shopify_customer_id
        FROM identity_journeys
        WHERE authoritative_shopify_customer_id LIKE $1 || '-customer-%'
        ORDER BY authoritative_shopify_customer_id DESC
        LIMIT 1
      `,
      [verificationTag]
    );

    const authoritativeShopifyCustomerId = journeyLookupResult.rows[0]?.authoritative_shopify_customer_id;

    if (!authoritativeShopifyCustomerId) {
      throw new Error('Unable to select an authoritative Shopify customer id for plan verification');
    }

    const nodeLookupResult = await client.query(
      `
        SELECT id, node_key
        FROM identity_nodes
        WHERE node_type = 'hashed_email'
          AND node_key LIKE $1 || '-email-%'
        ORDER BY node_key DESC
        LIMIT 1
      `,
      [verificationTag]
    );

    const nodeId = nodeLookupResult.rows[0]?.id;
    const nodeKey = nodeLookupResult.rows[0]?.node_key;

    if (!nodeId || !nodeKey) {
      throw new Error('Unable to select an identity node for plan verification');
    }

    const journeyIdResult = await client.query(
      `
        SELECT journey_id
        FROM identity_edges
        WHERE node_id = $1::uuid
          AND is_active = true
      `,
      [nodeId]
    );

    const journeyId = journeyIdResult.rows[0]?.journey_id;

    if (!journeyId) {
      throw new Error('Unable to select an identity journey edge for plan verification');
    }

    await explainUsesIndex(
      client,
      'journey_lookup',
      `
        SELECT id
        FROM identity_journeys
        WHERE authoritative_shopify_customer_id = $1
      `,
      [authoritativeShopifyCustomerId],
      REQUIRED_INDEXES.journey_lookup
    );

    await explainUsesIndex(
      client,
      'node_lookup',
      `
        SELECT id
        FROM identity_nodes
        WHERE node_type = 'hashed_email'
          AND node_key = $1
      `,
      [nodeKey],
      REQUIRED_INDEXES.node_lookup
    );

    await explainUsesIndex(
      client,
      'active_edge_lookup',
      `
        SELECT journey_id
        FROM identity_edges
        WHERE node_id = $1::uuid
          AND is_active = true
      `,
      [nodeId],
      REQUIRED_INDEXES.active_edge_lookup
    );

    await explainUsesIndex(
      client,
      'journey_edge_lookup',
      `
        SELECT node_id
        FROM identity_edges
        WHERE journey_id = $1::uuid
          AND is_active = true
        ORDER BY precedence_rank DESC, last_observed_at DESC
        LIMIT 20
      `,
      [journeyId],
      REQUIRED_INDEXES.journey_edge_lookup
    );

    await explainUsesIndex(
      client,
      'lookback_lookup',
      `
        SELECT id
        FROM identity_journeys
        WHERE lookback_window_expires_at >= now() - interval '1 day'
        ORDER BY lookback_window_expires_at DESC
        LIMIT 100
      `,
      [],
      REQUIRED_INDEXES.lookback_lookup
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
