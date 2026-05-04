import pg from "pg";

const { Client } = pg;

const REQUIRED_INDEXES = {
	shopify_receipts: "shopify_webhook_receipts_payload_lookup_idx",
	shopify_orders: "shopify_orders_payload_lookup_idx",
	meta_connections: "meta_ads_connections_raw_account_lookup_idx",
	meta_spend: "meta_ads_raw_spend_records_payload_lookup_idx",
	google_connections: "google_ads_connections_raw_customer_lookup_idx",
	google_spend: "google_ads_raw_spend_records_payload_lookup_idx",
};

function requireDatabaseUrl() {
	if (!process.env.DATABASE_URL) {
		throw new Error(
			"DATABASE_URL is required for raw payload query plan verification",
		);
	}

	return process.env.DATABASE_URL;
}

function collectPlanNodes(node, names = []) {
	if (!node || typeof node !== "object") {
		return names;
	}

	if (typeof node["Index Name"] === "string") {
		names.push(node["Index Name"]);
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
	const planRoot = result.rows[0]["QUERY PLAN"][0]?.Plan;
	const indexNames = collectPlanNodes(planRoot);

	if (!indexNames.includes(expectedIndex)) {
		throw new Error(
			`${label} did not use ${expectedIndex}. Planner used: ${indexNames.length > 0 ? indexNames.join(", ") : "no index"}`,
		);
	}

	process.stdout.write(`${label}: ${expectedIndex}\n`);
}

async function seedShopifyRawTables(client, tag) {
	await client.query(
		`
      INSERT INTO shopify_orders (
        shopify_order_id,
        shopify_order_number,
        currency_code,
        subtotal_price,
        total_price,
        payload_source,
        payload_received_at,
        payload_external_id,
        payload_size_bytes,
        payload_hash,
        raw_payload
      )
      SELECT
        $1 || '-order-' || gs::text,
        gs::text,
        'USD',
        10.00,
        12.50,
        'shopify_order',
        now() - (gs || ' minutes')::interval,
        $1 || '-order-' || (gs % 250)::text,
        2,
        md5(($1 || '-order-' || gs::text)),
        jsonb_build_object('id', $1 || '-order-' || gs::text, 'ordinal', gs)
      FROM generate_series(1, 5000) AS gs
    `,
		[tag],
	);

	await client.query(
		`
      INSERT INTO shopify_webhook_receipts (
        topic,
        shop_domain,
        webhook_id,
        payload_hash,
        received_at,
        status,
        raw_payload,
        payload_source,
        payload_size_bytes,
        payload_external_id
      )
      SELECT
        'orders/create',
        'example-shop.myshopify.com',
        $1 || '-webhook-' || gs::text,
        md5(($1 || '-receipt-' || gs::text)),
        now() - (gs || ' minutes')::interval,
        'processed',
        jsonb_build_object('id', $1 || '-order-' || (gs % 250)::text, 'ordinal', gs),
        'shopify_webhook',
        2,
        $1 || '-order-' || (gs % 250)::text
      FROM generate_series(1, 5000) AS gs
    `,
		[tag],
	);
}

async function seedMetaRawTables(client, tag) {
	await client.query(
		`
      INSERT INTO meta_ads_connections (
        id,
        ad_account_id,
        access_token_encrypted,
        token_type,
        granted_scopes,
        status,
        account_name,
        account_currency,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_external_id,
        raw_account_payload_size_bytes,
        raw_account_payload_hash
      )
      VALUES (
        91001,
        $1 || '-meta-account',
        pgp_sym_encrypt('meta-token', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        'Bearer',
        ARRAY['ads_read']::text[],
        'active',
        'Meta Account',
        'USD',
        jsonb_build_object('id', $1 || '-meta-account'),
        'meta_ads_account',
        now(),
        $1 || '-meta-account',
        2,
        md5($1 || '-meta-account')
      )
      ON CONFLICT (id) DO NOTHING
    `,
		[tag],
	);

	await client.query(
		`
      INSERT INTO meta_ads_connections (
        id,
        ad_account_id,
        access_token_encrypted,
        token_type,
        granted_scopes,
        status,
        account_name,
        account_currency,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_external_id,
        raw_account_payload_size_bytes,
        raw_account_payload_hash
      )
      SELECT
        91100 + gs,
        $1 || '-meta-extra-' || gs::text,
        pgp_sym_encrypt('meta-token', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        'Bearer',
        ARRAY['ads_read']::text[],
        'active',
        'Meta Account ' || gs::text,
        'USD',
        jsonb_build_object('id', $1 || '-meta-extra-' || gs::text),
        'meta_ads_account',
        now() - (gs || ' minutes')::interval,
        $1 || '-meta-extra-' || gs::text,
        2,
        md5(($1 || '-meta-extra-' || gs::text))
      FROM generate_series(1, 3000) AS gs
      ON CONFLICT (id) DO NOTHING
    `,
		[tag],
	);

	await client.query(
		`
      INSERT INTO meta_ads_sync_jobs (
        id,
        connection_id,
        sync_date,
        status,
        completed_at
      )
      VALUES (91001, 91001, current_date, 'completed', now())
      ON CONFLICT (id) DO NOTHING
    `,
	);

	await client.query(
		`
      INSERT INTO meta_ads_raw_spend_records (
        connection_id,
        sync_job_id,
        report_date,
        level,
        entity_id,
        payload_external_id,
        currency,
        spend,
        impressions,
        clicks,
        raw_payload,
        payload_source,
        payload_received_at,
        payload_size_bytes,
        payload_hash
      )
      SELECT
        91001,
        91001,
        current_date - ((gs - 1) / 400),
        'campaign',
        $1 || '-meta-campaign-' || (gs % 400)::text,
        $1 || '-meta-campaign-' || (gs % 400)::text,
        'USD',
        10.00,
        100,
        5,
        jsonb_build_object('campaign_id', $1 || '-meta-campaign-' || (gs % 400)::text, 'ordinal', gs),
        'meta_ads_insights',
        now() - (gs || ' minutes')::interval,
        2,
        md5(($1 || '-meta-row-' || gs::text))
      FROM generate_series(1, 8000) AS gs
    `,
		[tag],
	);
}

async function seedGoogleRawTables(client, tag) {
	await client.query(
		`
      INSERT INTO google_ads_connections (
        id,
        customer_id,
        developer_token_encrypted,
        client_id,
        client_secret_encrypted,
        refresh_token_encrypted,
        status,
        customer_descriptive_name,
        currency_code,
        raw_customer_data,
        raw_customer_source,
        raw_customer_received_at,
        raw_customer_external_id,
        raw_customer_payload_size_bytes,
        raw_customer_payload_hash
      )
      VALUES (
        92001,
        $1 || '-google-customer',
        pgp_sym_encrypt('dev-token', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        'client-id',
        pgp_sym_encrypt('client-secret', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        pgp_sym_encrypt('refresh-token', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        'active',
        'Google Account',
        'USD',
        jsonb_build_object('customer', jsonb_build_object('id', $1 || '-google-customer')),
        'google_ads_customer',
        now(),
        $1 || '-google-customer',
        2,
        md5($1 || '-google-customer')
      )
      ON CONFLICT (id) DO NOTHING
    `,
		[tag],
	);

	await client.query(
		`
      INSERT INTO google_ads_connections (
        id,
        customer_id,
        developer_token_encrypted,
        client_id,
        client_secret_encrypted,
        refresh_token_encrypted,
        status,
        customer_descriptive_name,
        currency_code,
        raw_customer_data,
        raw_customer_source,
        raw_customer_received_at,
        raw_customer_external_id,
        raw_customer_payload_size_bytes,
        raw_customer_payload_hash
      )
      SELECT
        92100 + gs,
        $1 || '-google-extra-' || gs::text,
        pgp_sym_encrypt('dev-token', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        'client-id',
        pgp_sym_encrypt('client-secret', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        pgp_sym_encrypt('refresh-token', 'test-key', 'cipher-algo=aes256, compress-algo=0'),
        'active',
        'Google Account ' || gs::text,
        'USD',
        jsonb_build_object('customer', jsonb_build_object('id', $1 || '-google-extra-' || gs::text)),
        'google_ads_customer',
        now() - (gs || ' minutes')::interval,
        $1 || '-google-extra-' || gs::text,
        2,
        md5(($1 || '-google-extra-' || gs::text))
      FROM generate_series(1, 3000) AS gs
      ON CONFLICT (id) DO NOTHING
    `,
		[tag],
	);

	await client.query(
		`
      INSERT INTO google_ads_sync_jobs (
        id,
        connection_id,
        sync_date,
        status,
        completed_at
      )
      VALUES (92001, 92001, current_date, 'completed', now())
      ON CONFLICT (id) DO NOTHING
    `,
	);

	await client.query(
		`
      INSERT INTO google_ads_raw_spend_records (
        connection_id,
        sync_job_id,
        report_date,
        level,
        entity_id,
        payload_external_id,
        currency,
        spend,
        impressions,
        clicks,
        raw_payload,
        payload_source,
        payload_received_at,
        payload_size_bytes,
        payload_hash
      )
      SELECT
        92001,
        92001,
        current_date - ((gs - 1) / 400),
        'campaign',
        $1 || '-google-campaign-' || (gs % 400)::text,
        $1 || '-google-campaign-' || (gs % 400)::text,
        'USD',
        11.00,
        101,
        6,
        jsonb_build_object('campaign', jsonb_build_object('id', $1 || '-google-campaign-' || (gs % 400)::text), 'ordinal', gs),
        'google_ads_api',
        now() - (gs || ' minutes')::interval,
        2,
        md5(($1 || '-google-row-' || gs::text))
      FROM generate_series(1, 8000) AS gs
    `,
		[tag],
	);
}

async function main() {
	const client = new Client({ connectionString: requireDatabaseUrl() });
	const verificationTag = `raw-plan-${Date.now()}`;

	await client.connect();

	try {
		await client.query("BEGIN");

		await seedShopifyRawTables(client, verificationTag);
		await seedMetaRawTables(client, verificationTag);
		await seedGoogleRawTables(client, verificationTag);

		await client.query("ANALYZE shopify_webhook_receipts");
		await client.query("ANALYZE shopify_orders");
		await client.query("ANALYZE meta_ads_connections");
		await client.query("ANALYZE meta_ads_raw_spend_records");
		await client.query("ANALYZE google_ads_connections");
		await client.query("ANALYZE google_ads_raw_spend_records");

		await explainUsesIndex(
			client,
			"shopify_receipts_lookup",
			`
        SELECT id
        FROM shopify_webhook_receipts
        WHERE payload_source = $1
          AND payload_external_id = $2
        ORDER BY received_at DESC
        LIMIT 20
      `,
			["shopify_webhook", `${verificationTag}-order-7`],
			REQUIRED_INDEXES.shopify_receipts,
		);

		await explainUsesIndex(
			client,
			"shopify_orders_lookup",
			`
        SELECT id
        FROM shopify_orders
        WHERE payload_source = $1
          AND payload_external_id = $2
        ORDER BY payload_received_at DESC
        LIMIT 1
      `,
			["shopify_order", `${verificationTag}-order-7`],
			REQUIRED_INDEXES.shopify_orders,
		);

		await explainUsesIndex(
			client,
			"meta_connection_lookup",
			`
        SELECT id
        FROM meta_ads_connections
        WHERE raw_account_source = $1
          AND raw_account_external_id = $2
        ORDER BY raw_account_received_at DESC
        LIMIT 1
      `,
			["meta_ads_account", `${verificationTag}-meta-account`],
			REQUIRED_INDEXES.meta_connections,
		);

		await explainUsesIndex(
			client,
			"meta_spend_lookup",
			`
        SELECT id
        FROM meta_ads_raw_spend_records
        WHERE payload_source = $1
          AND payload_external_id = $2
        ORDER BY payload_received_at DESC
        LIMIT 20
      `,
			["meta_ads_insights", `${verificationTag}-meta-campaign-17`],
			REQUIRED_INDEXES.meta_spend,
		);

		await explainUsesIndex(
			client,
			"google_connection_lookup",
			`
        SELECT id
        FROM google_ads_connections
        WHERE raw_customer_source = $1
          AND raw_customer_external_id = $2
        ORDER BY raw_customer_received_at DESC
        LIMIT 1
      `,
			["google_ads_customer", `${verificationTag}-google-customer`],
			REQUIRED_INDEXES.google_connections,
		);

		await explainUsesIndex(
			client,
			"google_spend_lookup",
			`
        SELECT id
        FROM google_ads_raw_spend_records
        WHERE payload_source = $1
          AND payload_external_id = $2
        ORDER BY payload_received_at DESC
        LIMIT 20
      `,
			["google_ads_api", `${verificationTag}-google-campaign-17`],
			REQUIRED_INDEXES.google_spend,
		);
	} finally {
		await client.query("ROLLBACK");
		await client.end();
	}
}

main().catch((error) => {
	process.stderr.write(
		`${error instanceof Error ? error.stack : String(error)}\n`,
	);
	process.exit(1);
});
