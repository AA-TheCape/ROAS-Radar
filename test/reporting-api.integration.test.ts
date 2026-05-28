import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";
process.env.REPORTING_API_TOKEN = "test-reporting-token";
process.env.DEFAULT_ORGANIZATION_ID = "77";
process.env.SHOPIFY_APP_API_SECRET ??= "test-app-secret";
process.env.SHOPIFY_WEBHOOK_SECRET ??= "test-webhook-secret";

const poolModule = await import("../src/db/pool.js");
const serverModule = await import("../src/server.js");
const harnessModule = await import("./e2e-harness.js");

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const { resetE2EDatabase } = harnessModule;
const { buildRawPayloadFixture } = await import("./integration-test-helpers.js");

function buildHeaders(): Record<string, string> {
	return {
		authorization: "Bearer test-reporting-token",
	};
}

async function requestJson(
	server: ReturnType<typeof createServer>,
	path: string,
	headers = buildHeaders(),
) {
	const address = server.address() as AddressInfo;
	const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
		headers,
	});
	const body = await response.json();

	return { response, body };
}

async function seedMetaConnection(adAccountId: string): Promise<number> {
	const rawAccountFixture = buildRawPayloadFixture(
		{
			id: adAccountId,
			name: `Account ${adAccountId}`,
			currency: "USD",
		},
		adAccountId,
	);
	const result = await pool.query<{ id: number }>(
		`
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        account_currency,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_payload_size_bytes,
        raw_account_payload_hash,
        raw_account_external_id
      )
      VALUES (
        $1,
        '\\x01'::bytea,
        'USD',
        $2::jsonb,
        'meta_ads_account',
        '2026-04-10T15:55:00.000Z',
        $3,
        $4,
        $5
      )
      RETURNING id
    `,
		[
			adAccountId,
			rawAccountFixture.rawPayloadJson,
			rawAccountFixture.payloadSizeBytes,
			rawAccountFixture.payloadHash,
			rawAccountFixture.payloadExternalId,
		],
	);

	return result.rows[0].id;
}

async function seedMetaOrderValueSyncJob(
	connectionId: number,
	syncDate: string,
): Promise<number> {
	const result = await pool.query<{ id: number }>(
		`
      INSERT INTO meta_ads_order_value_sync_jobs (connection_id, sync_date)
      VALUES ($1, $2::date)
      RETURNING id
    `,
		[connectionId, syncDate],
	);

	return result.rows[0].id;
}

async function seedMetaViewThroughAggregate(params: {
	connectionId: number;
	syncJobId: number;
	reportDate: string;
	campaignId: string;
	campaignName: string;
	attributedRevenue: number;
	purchaseCount: number;
	spend: number;
}): Promise<void> {
	await pool.query(
		`
      INSERT INTO meta_ads_order_value_aggregates (
        organization_id,
        meta_connection_id,
        sync_job_id,
        ad_account_id,
        report_date,
        raw_date_start,
        raw_date_stop,
        campaign_id,
        campaign_name,
        attributed_revenue,
        purchase_count,
        spend,
        purchase_roas,
        currency,
        canonical_action_type,
        canonical_selection_mode,
        raw_action_values,
        raw_actions,
        raw_revenue_record_ids,
        source_synced_at,
        action_report_time,
        use_account_attribution_setting
      )
      VALUES (
        77,
        $1,
        $2,
        '123456789',
        $3::date,
        $3::date,
        $3::date,
        $4,
        $5,
        $6,
        $7,
        $8,
        CASE WHEN $8::numeric > 0 THEN ($6::numeric / $8::numeric) ELSE NULL END,
        'USD',
        'purchase',
        'priority',
        '[]'::jsonb,
        '[]'::jsonb,
        '[]'::jsonb,
        '2026-04-10T16:00:00.000Z',
        'impression',
        true
      )
    `,
		[
			params.connectionId,
			params.syncJobId,
			params.reportDate,
			params.campaignId,
			params.campaignName,
			params.attributedRevenue,
			params.purchaseCount,
			params.spend,
		],
	);
}

test("reporting summary reads persisted daily aggregates from PostgreSQL", async () => {
	await resetE2EDatabase();
	await pool.query(
		`INSERT INTO daily_reporting_metrics (
      metric_date,
      attribution_model,
      source,
      medium,
      campaign,
      content,
      term,
      visits,
      attributed_orders,
      attributed_revenue,
      spend,
      impressions,
      clicks,
      new_customer_orders,
      returning_customer_orders,
      new_customer_revenue,
      returning_customer_revenue,
      last_computed_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now())`,
		[
			"2026-04-10",
			"last_touch",
			"google",
			"cpc",
			"spring-sale",
			"hero-ad-1",
			"widget",
			42,
			3,
			"390.00",
			"0.00",
			0,
			0,
			1,
			2,
			"120.00",
			"270.00",
		],
	);

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/reporting/summary?startDate=2026-04-10&endDate=2026-04-10&source=google&campaign=spring-sale",
		);

		assert.equal(response.status, 200);
		assert.deepEqual(body, {
			range: {
				startDate: "2026-04-10",
				endDate: "2026-04-10",
			},
			reportingMode: "clicks",
			reportingModeLabel: "Click attribution",
			totalsLabel: "Click attribution",
			totalsCanonical: true,
			totalsDescription: "Canonical reporting totals from click-attributed order credits.",
			totals: {
				visits: 42,
				orders: 3,
				revenue: 390,
				spend: 0,
				conversionRate: 3 / 42,
				roas: null,
			},
			comparisonTotals: {
				combined: {
					label: "Non-canonical comparison total",
					canonical: false,
					description:
						"Comparison-only sum of click attribution and deterministic view attribution; do not treat as canonical revenue.",
					totals: {
						visits: 42,
						orders: 3,
						revenue: 390,
						spend: 0,
						conversionRate: 3 / 42,
						roas: null,
					},
				},
			},
			layers: {
				clicks: {
					label: "Click attribution",
					canonical: true,
					description: "Canonical reporting totals from click-attributed order credits.",
					totals: {
						visits: 42,
						orders: 3,
						revenue: 390,
						spend: 0,
						conversionRate: 3 / 42,
						roas: null,
					},
				},
				deterministicViews: {
					label: "Deterministic view layer",
					canonical: false,
					description:
						"Layer-only Meta API-verified deterministic view/impression attribution.",
					totals: {
						visits: 0,
						orders: 0,
						revenue: 0,
						spend: 0,
						conversionRate: 0,
						roas: null,
					},
				},
				metaViewThrough: {
					label: "Meta API view-through",
					canonical: false,
					description:
						"Meta API-reported view-through purchase revenue, purchases, and ROAS from impression-time reporting.",
					totals: {
						visits: 0,
						orders: 0,
						revenue: 0,
						spend: 0,
						conversionRate: 0,
						roas: null,
					},
				},
			},
		});
	} finally {
		await closeServer(server);
		await resetE2EDatabase();
	}
});

test("reporting summary exposes Meta API view-through totals as a separate layer", async () => {
	await resetE2EDatabase();
	const connectionId = await seedMetaConnection("123456789");
	const syncJobId = await seedMetaOrderValueSyncJob(connectionId, "2026-04-10");
	await seedMetaViewThroughAggregate({
		connectionId,
		syncJobId,
		reportDate: "2026-04-10",
		campaignId: "cmp_view",
		campaignName: "View Prospecting",
		attributedRevenue: 250,
		purchaseCount: 5,
		spend: 100,
	});

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/reporting/summary?startDate=2026-04-10&endDate=2026-04-10&reportingMode=meta_view_through&source=meta&campaign=cmp_view",
		);

		assert.equal(response.status, 200);
		assert.equal(body.reportingMode, "meta_view_through");
		assert.deepEqual(body.totals, {
			visits: 0,
			orders: 5,
			revenue: 250,
			spend: 100,
			conversionRate: 0,
			roas: 2.5,
		});
		assert.deepEqual(body.layers.metaViewThrough, {
			label: "Meta API view-through",
			canonical: false,
			description:
				"Meta API-reported view-through purchase revenue, purchases, and ROAS from impression-time reporting.",
			totals: {
				visits: 0,
				orders: 5,
				revenue: 250,
				spend: 100,
				conversionRate: 0,
				roas: 2.5,
			},
		});
		assert.equal(body.layers.clicks.totals.revenue, 0);
		assert.equal(body.layers.deterministicViews.totals.revenue, 0);
	} finally {
		await closeServer(server);
		await resetE2EDatabase();
	}
});

test("reporting spend details and lowest buckets are scoped to the requested date range", async () => {
	await resetE2EDatabase();
	await pool.query(
		`INSERT INTO daily_reporting_metrics (
      metric_date,
      attribution_model,
      source,
      medium,
      campaign,
      content,
      term,
      visits,
      attributed_orders,
      attributed_revenue,
      spend,
      impressions,
      clicks,
      new_customer_orders,
      returning_customer_orders,
      new_customer_revenue,
      returning_customer_revenue,
      last_computed_at
    ) VALUES
      ($1, $2, $3, $4, $5, 'unknown', 'unknown', $6, $7, $8, $9, 0, 0, 0, 0, 0, 0, now()),
      ($10, $11, $12, $13, $14, 'unknown', 'unknown', $15, $16, $17, $18, 0, 0, 0, 0, 0, 0, now()),
      ($19, $20, $21, $22, $23, 'unknown', 'unknown', $24, $25, $26, $27, 0, 0, 0, 0, 0, 0, now())`,
		[
			"2026-04-08",
			"last_touch",
			"google",
			"cpc",
			"brand-search",
			120,
			4,
			"540.00",
			"210.00",
			"2026-04-09",
			"last_touch",
			"google",
			"cpc",
			"spring-search",
			300,
			10,
			"1800.00",
			"700.00",
			"2026-04-10",
			"last_touch",
			"meta",
			"paid_social",
			"prospecting-us",
			180,
			6,
			"620.00",
			"450.00",
		],
	);

	const server = createServer();

	try {
		const spendDetails = await requestJson(
			server,
			"/api/reporting/spend-details?startDate=2026-04-09&endDate=2026-04-10",
		);
		const timeseries = await requestJson(
			server,
			"/api/reporting/timeseries?startDate=2026-04-09&endDate=2026-04-10&groupBy=campaign",
		);

		assert.equal(spendDetails.response.status, 200);
		assert.deepEqual(spendDetails.body, {
			summary: {
				totalSpend: 1150,
				activeChannels: 2,
				activeCampaigns: 2,
				averageDailySpend: 575,
				topChannel: {
					source: "google",
					medium: "cpc",
					channel: "google / cpc",
					spend: 700,
				},
			},
			groups: [
				{
					source: "google",
					medium: "cpc",
					channel: "google / cpc",
					subtotal: 700,
					campaigns: [
						{
							campaign: "spring-search",
							spend: 700,
						},
					],
				},
				{
					source: "meta",
					medium: "paid_social",
					channel: "meta / paid_social",
					subtotal: 450,
					campaigns: [
						{
							campaign: "prospecting-us",
							spend: 450,
						},
					],
				},
			],
			totalSpend: 1150,
		});

		assert.equal(timeseries.response.status, 200);
		assert.deepEqual(timeseries.body, {
			points: [
				{
					date: "prospecting-us",
					visits: 180,
					orders: 6,
					revenue: 620,
				},
				{
					date: "spring-search",
					visits: 300,
					orders: 10,
					revenue: 1800,
				},
			],
			lowestBuckets: [
				{
					bucket: "prospecting-us",
					visits: 180,
					orders: 6,
					revenue: 620,
					spend: 450,
					conversionRate: 6 / 180,
					roas: 620 / 450,
				},
				{
					bucket: "spring-search",
					visits: 300,
					orders: 10,
					revenue: 1800,
					spend: 700,
					conversionRate: 10 / 300,
					roas: 1800 / 700,
				},
			],
		});
	} finally {
		await closeServer(server);
		await resetE2EDatabase();
	}
});

test('reporting orders only returns online store Shopify orders', async () => {
  await resetE2EDatabase();
  await pool.query(
    `INSERT INTO shopify_orders (
      shopify_order_id,
      shopify_order_number,
      currency_code,
      subtotal_price,
      total_price,
      processed_at,
      source_name,
      raw_payload,
      payload_source,
      payload_external_id,
      payload_size_bytes,
      payload_hash
    ) VALUES
      ($1, $2, 'USD', '75.00', '80.00', $3, 'web', '{}'::jsonb, 'shopify_order', $1, 2, '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
      ($4, $5, 'USD', '45.00', '50.00', $6, 'pos', '{}'::jsonb, 'shopify_order', $4, 2, '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a')`,
    [
      'web-order-1',
      '18387',
      '2026-04-10T13:00:00.000Z',
      'pos-order-1',
      '18388',
      '2026-04-10T14:00:00.000Z'
    ]
  );

  await pool.query(
    `INSERT INTO attribution_order_credits (
      shopify_order_id,
      attribution_model,
      touchpoint_position,
      session_id,
      touchpoint_occurred_at,
      attributed_source,
      attributed_medium,
      attributed_campaign,
      credit_weight,
      revenue_credit,
      is_primary,
      attribution_reason,
      match_source,
      confidence_label,
      model_version
    ) VALUES
      ($1, 'last_touch', 1, NULL, $2, 'facebook', 'paid_social', 'prospecting-us', '1.0', '80.00', true, 'matched_by_checkout_token', 'matched_by_checkout_token', 'high', 1),
      ($3, 'last_touch', 1, NULL, $4, 'pos', 'offline', 'retail', '1.0', '50.00', true, 'matched_by_checkout_token', 'matched_by_checkout_token', 'high', 1)`,
    [
      'web-order-1',
      '2026-04-10T12:55:00.000Z',
      'pos-order-1',
      '2026-04-10T13:55:00.000Z'
    ]
  );

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/orders?startDate=2026-04-10&endDate=2026-04-10&limit=10'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      rows: [
        {
          shopifyOrderId: 'web-order-1',
          processedAt: '2026-04-10T13:00:00.000Z',
          orderOccurredAtUtc: '2026-04-10T13:00:00.000Z',
          totalPrice: 80,
          source: 'facebook',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          attributionReason: 'unattributed',
          primaryCreditAttributionReason: 'matched_by_checkout_token',
          attributionTier: 'unattributed',
          attributionTierLabel: 'Unattributed',
          attributionTierDescription:
            'No eligible first-party, Shopify hint, or GA4 fallback match qualified, or the required timing data could not be normalized.',
          attributionSource: null,
          attributionMatchedAt: null,
          confidenceScore: null,
          sessionId: null
        }
      ]
    });
  } finally {
    await closeServer(server);
    await resetE2EDatabase();
  }
});

test.after(async () => {
	await pool.end();
});
