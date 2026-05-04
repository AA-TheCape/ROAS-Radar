import pg from 'pg';

const { Client } = pg;

const REQUIRED_INDEXES = {
  scoped_lookup: 'ad_platform_entity_metadata_lookup_idx',
  entity_lookup: 'ad_platform_entity_metadata_entity_lookup_idx'
};

function requireDatabaseUrl() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required for campaign metadata query plan verification');
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
  const verificationTag = `metadata-plan-${Date.now()}`;

  await client.connect();

  try {
    await client.query('BEGIN');

    await client.query(
      `
        INSERT INTO ad_platform_entity_metadata (
          tenant_id,
          workspace_id,
          platform,
          account_id,
          entity_type,
          entity_id,
          latest_name,
          last_seen_at,
          updated_at
        )
        SELECT
          CASE WHEN gs % 4 = 0 THEN 'tenant-enterprise' ELSE NULL END,
          CASE WHEN gs % 5 = 0 THEN 'workspace-reporting' ELSE NULL END,
          CASE WHEN gs % 2 = 0 THEN 'google_ads' ELSE 'meta_ads' END,
          CASE WHEN gs % 2 = 0 THEN 'google-account-' || (gs % 40)::text ELSE 'meta-account-' || (gs % 40)::text END,
          CASE
            WHEN gs % 3 = 0 THEN 'campaign'
            WHEN gs % 3 = 1 THEN 'adset'
            ELSE 'ad'
          END,
          $1 || '-entity-' || gs::text,
          'Entity Name ' || gs::text,
          now() - (gs || ' minutes')::interval,
          now() - (gs || ' minutes')::interval
        FROM generate_series(1, 12000) AS gs
      `,
      [verificationTag]
    );

    await client.query(
      `
        INSERT INTO ad_platform_entity_metadata (
          tenant_id,
          workspace_id,
          platform,
          account_id,
          entity_type,
          entity_id,
          latest_name,
          last_seen_at,
          updated_at
        )
        VALUES
          (
            'tenant-enterprise',
            'workspace-reporting',
            'google_ads',
            'google-account-7',
            'campaign',
            $1,
            'Scoped Lookup Target',
            now(),
            now()
          ),
          (
            NULL,
            NULL,
            'meta_ads',
            'meta-account-9',
            'adset',
            $2,
            'Entity Lookup Target',
            now(),
            now()
          )
      `,
      [`${verificationTag}-scoped`, `${verificationTag}-entity`]
    );

    await client.query('ANALYZE ad_platform_entity_metadata');

    await explainUsesIndex(
      client,
      'scoped_lookup',
      `
        SELECT latest_name
        FROM ad_platform_entity_metadata
        WHERE platform = $1
          AND account_id = $2
          AND entity_type = $3
          AND entity_id = $4
          AND tenant_id = $5
          AND workspace_id = $6
      `,
      [
        'google_ads',
        'google-account-7',
        'campaign',
        `${verificationTag}-scoped`,
        'tenant-enterprise',
        'workspace-reporting'
      ],
      REQUIRED_INDEXES.scoped_lookup
    );

    await explainUsesIndex(
      client,
      'entity_lookup',
      `
        SELECT latest_name
        FROM ad_platform_entity_metadata
        WHERE platform = $1
          AND entity_id = $2
          AND account_id = $3
          AND entity_type = $4
      `,
      ['meta_ads', `${verificationTag}-entity`, 'meta-account-9', 'adset'],
      REQUIRED_INDEXES.entity_lookup
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
