import pg from "pg";

const { Client } = pg;

const REQUIRED_INDEX_SUFFIXES = {
	customerIdentityLookup: "_customer_identity_lookup_idx",
	emailHashLookup: "_email_hash_lookup_idx",
	transactionLookup: "_transaction_lookup_idx",
};

function requireDatabaseUrl() {
	if (!process.env.DATABASE_URL) {
		throw new Error(
			"DATABASE_URL is required for GA4 fallback query plan verification",
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

async function explainUsesIndexSuffix(
	client,
	label,
	sql,
	params,
	expectedSuffix,
) {
	const result = await client.query(`EXPLAIN (FORMAT JSON) ${sql}`, params);
	const planRoot = result.rows[0]["QUERY PLAN"][0]?.Plan;
	const indexNames = collectPlanNodes(planRoot);

	if (!indexNames.some((indexName) => indexName.endsWith(expectedSuffix))) {
		throw new Error(
			`${label} did not use an index ending with ${expectedSuffix}. Planner used: ${
				indexNames.length > 0 ? indexNames.join(", ") : "no index"
			}`,
		);
	}

	process.stdout.write(`${label}: ${expectedSuffix}\n`);
}

async function main() {
	const client = new Client({ connectionString: requireDatabaseUrl() });
	const verificationTag = `ga4-plan-${Date.now()}`;
	const emailHash =
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

	await client.connect();

	try {
		await client.query("BEGIN");
		await client.query(
			`
        INSERT INTO customer_identities (
          id,
          hashed_email,
          shopify_customer_id,
          created_at,
          updated_at,
          last_stitched_at
        )
        VALUES (
          '11111111-1111-4111-8111-111111111111'::uuid,
          $1,
          $2,
          now(),
          now(),
          now()
        )
        ON CONFLICT (id) DO NOTHING
      `,
			[emailHash, verificationTag],
		);

		await client.query(
			`
        SELECT ensure_ga4_fallback_candidate_partition('2026-04-01'::date)
      `,
		);

		await client.query(
			`
        INSERT INTO ga4_fallback_candidates (
          candidate_key,
          occurred_at,
          ga4_user_key,
          ga4_client_id,
          ga4_session_id,
          transaction_id,
          email_hash,
          customer_identity_id,
          source,
          medium,
          campaign,
          click_id_type,
          click_id_value,
          session_has_required_fields,
          source_export_hour,
          source_dataset,
          source_table_type,
          retained_until
        )
        SELECT
          md5($1 || '-' || gs::text),
          '2026-04-26T12:00:00.000Z'::timestamptz - (gs || ' minutes')::interval,
          $1 || '-user-' || gs::text,
          $1 || '-client-' || gs::text,
          $1 || '-session-' || gs::text,
          CASE WHEN gs % 2 = 0 THEN $1 || '-order-42' ELSE $1 || '-order-' || gs::text END,
          CASE WHEN gs % 3 = 0 THEN $2 ELSE NULL END,
          CASE WHEN gs % 5 = 0 THEN '11111111-1111-4111-8111-111111111111'::uuid ELSE NULL END,
          CASE WHEN gs % 2 = 0 THEN 'google' ELSE 'email' END,
          CASE WHEN gs % 2 = 0 THEN 'cpc' ELSE 'newsletter' END,
          'campaign-' || (gs % 20)::text,
          CASE WHEN gs % 2 = 0 THEN 'gclid' ELSE NULL END,
          CASE WHEN gs % 2 = 0 THEN 'gclid-' || gs::text ELSE NULL END,
          true,
          '2026-04-26T12:00:00.000Z'::timestamptz,
          'ga4_export',
          'events',
          now() + interval '10 days'
        FROM generate_series(1, 5000) AS gs
        ON CONFLICT (candidate_key, occurred_at) DO NOTHING
      `,
			[verificationTag, emailHash],
		);

		await client.query("ANALYZE customer_identities");
		await client.query("ANALYZE ga4_fallback_candidates");

		await explainUsesIndexSuffix(
			client,
			"customer_identity_lookup",
			`
        SELECT candidate_key
        FROM ga4_fallback_candidates
        WHERE customer_identity_id = $1::uuid
          AND occurred_at <= $2::timestamptz
          AND occurred_at >= $2::timestamptz - interval '7 days'
        ORDER BY occurred_at DESC, ga4_session_id ASC
        LIMIT 20
      `,
			["11111111-1111-4111-8111-111111111111", "2026-04-27T12:00:00.000Z"],
			REQUIRED_INDEX_SUFFIXES.customerIdentityLookup,
		);

		await explainUsesIndexSuffix(
			client,
			"email_hash_lookup",
			`
        SELECT candidate_key
        FROM ga4_fallback_candidates
        WHERE email_hash = $1
          AND occurred_at <= $2::timestamptz
          AND occurred_at >= $2::timestamptz - interval '7 days'
        ORDER BY occurred_at DESC, ga4_session_id ASC
        LIMIT 20
      `,
			[emailHash, "2026-04-27T12:00:00.000Z"],
			REQUIRED_INDEX_SUFFIXES.emailHashLookup,
		);

		await explainUsesIndexSuffix(
			client,
			"transaction_lookup",
			`
        SELECT candidate_key
        FROM ga4_fallback_candidates
        WHERE transaction_id = $1
          AND occurred_at <= $2::timestamptz
          AND occurred_at >= $2::timestamptz - interval '7 days'
        ORDER BY occurred_at DESC, ga4_session_id ASC
        LIMIT 20
      `,
			[`${verificationTag}-order-42`, "2026-04-27T12:00:00.000Z"],
			REQUIRED_INDEX_SUFFIXES.transactionLookup,
		);

		await client.query("ROLLBACK");
	} catch (error) {
		await client.query("ROLLBACK").catch(() => undefined);
		throw error;
	} finally {
		await client.end();
	}
}

main().catch((error) => {
	process.stderr.write(
		`${error instanceof Error ? error.stack : String(error)}\n`,
	);
	process.exit(1);
});
