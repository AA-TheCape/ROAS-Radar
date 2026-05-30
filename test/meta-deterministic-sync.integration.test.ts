import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test";
process.env.META_ADS_ENCRYPTION_KEY ??= "meta-encryption-key";
process.env.META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS = "1";
process.env.META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS = "1";

const { pool } = await import("../src/db/pool.js");
const { runMetaDeterministicSync } = await import(
	"../src/modules/meta-ads/deterministic-events.js"
);
const { resetE2EDatabase } = await import("./e2e-harness.js");

async function seedMetaConnection(input: {
	adAccountId: string;
	enabled: boolean;
	status?: string;
}): Promise<number> {
	const rawAccount = {
		id: input.adAccountId,
		name: `Meta Account ${input.adAccountId}`,
		currency: "USD",
	};
	const rawAccountJson = JSON.stringify(rawAccount);
	const result = await pool.query<{ id: number | string }>(
		`
      INSERT INTO meta_ads_connections (
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
        raw_account_payload_hash,
        deterministic_view_impression_sync_enabled
      )
      VALUES (
        $1,
        pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        'Bearer',
        ARRAY['ads_read']::text[],
        $9,
        $4,
        'USD',
        $5::jsonb,
        'meta_ads_account',
        now(),
        $1,
        $6,
        $7,
        $8
      )
      RETURNING id
    `,
		[
			input.adAccountId,
			`token-${input.adAccountId}`,
			process.env.META_ADS_ENCRYPTION_KEY,
			rawAccount.name,
			rawAccountJson,
			Buffer.byteLength(rawAccountJson, "utf8"),
			createHash("sha256").update(rawAccountJson).digest("hex"),
			input.enabled,
			input.status ?? "active",
		],
	);

	return Number(result.rows[0].id);
}

async function loadDeterministicCounts(): Promise<{
	jobs: string;
	rawRows: string;
	facts: string;
	aggregates: string;
	verifications: string;
	quarantine: string;
	checkpoints: string;
}> {
	const [jobs, rawRows, facts, aggregates, verifications, quarantine, checkpoints] =
		await Promise.all([
		pool.query<{ count: string }>(
			"SELECT count(*)::text AS count FROM meta_ads_deterministic_sync_jobs",
		),
		pool.query<{ count: string }>(
			"SELECT count(*)::text AS count FROM raw_deterministic_events",
		),
		pool.query<{ count: string }>(
			"SELECT count(*)::text AS count FROM deterministic_event_facts",
		),
		pool.query<{ count: string }>(
			"SELECT count(*)::text AS count FROM meta_ads_deterministic_attribution_aggregates",
		),
		pool.query<{ count: string }>(
			"SELECT count(*)::text AS count FROM deterministic_event_verification_statuses",
		),
		pool.query<{ count: string }>(
			"SELECT count(*)::text AS count FROM deterministic_event_evidence_quarantine",
		),
		pool.query<{ count: string }>(
			"SELECT count(*)::text AS count FROM meta_ads_deterministic_sync_checkpoints",
		),
		]);

	return {
		jobs: jobs.rows[0].count,
		rawRows: rawRows.rows[0].count,
		facts: facts.rows[0].count,
		aggregates: aggregates.rows[0].count,
		verifications: verifications.rows[0].count,
		quarantine: quarantine.rows[0].count,
		checkpoints: checkpoints.rows[0].count,
	};
}

test(
	"Meta deterministic sync fetches enabled accounts and duplicate-safe upserts events",
	{ concurrency: false },
	async () => {
		await resetE2EDatabase();
		await seedMetaConnection({ adAccountId: "123456789", enabled: true });
		await seedMetaConnection({ adAccountId: "987654321", enabled: false });
		await seedMetaConnection({
			adAccountId: "555555555",
			enabled: true,
			status: "revoked",
		});

		const originalFetch = globalThis.fetch;
		const requestedUrls: string[] = [];
		let transientFailuresRemaining = 1;
		globalThis.fetch = async (input: string | URL | Request) => {
			const url = input instanceof Request ? input.url : input.toString();
			requestedUrls.push(url);

			if (transientFailuresRemaining > 0) {
				transientFailuresRemaining -= 1;
				return new Response(JSON.stringify({ error: { message: "rate limited" } }), {
					status: 500,
					headers: { "content-type": "application/json" },
				});
			}

			return new Response(
				JSON.stringify({
					data: [
						{
							account_id: "123456789",
							account_name: "Meta Account",
							campaign_id: "campaign-1",
							campaign_name: "Campaign 1",
							adset_id: "adset-1",
							adset_name: "Ad Set 1",
							ad_id: "ad-1",
							ad_name: "Ad 1",
							date_start: "2026-05-26",
							date_stop: "2026-05-26",
							impressions: "12",
							video_play_actions: [{ action_type: "video_view", value: "3" }],
						},
						{
							account_id: "123456789",
							date_start: "2026-05-26",
							date_stop: "2026-05-26",
							impressions: "99",
						},
					],
				}),
				{
					status: 200,
					headers: {
						"content-type": "application/json",
						"x-fb-trace-id": "trace-123",
					},
				},
			);
		};

		try {
			const firstRun = await runMetaDeterministicSync({
				now: new Date("2026-05-27T12:00:00Z"),
				triggerSource: "test",
			});
			assert.equal(firstRun.succeededJobs, 1);
			assert.equal(firstRun.failedJobs, 0);
			assert.equal(firstRun.recordsReceived, 2);
			assert.equal(firstRun.rawRowsFetched, 2);
			assert.equal(firstRun.aggregateRowsUpserted, 2);
			assert.equal(firstRun.apiRequestCount, 2);
			assert.equal(requestedUrls.length, 2);
			assert.match(requestedUrls[1], /act_123456789\/insights/);
			assert.doesNotMatch(requestedUrls[0], /987654321/);
			assert.ok(requestedUrls.every((url) => !url.includes("555555555")));

			assert.deepEqual(await loadDeterministicCounts(), {
				jobs: "1",
				rawRows: "2",
				facts: "2",
				aggregates: "2",
				verifications: "2",
				quarantine: "1",
				checkpoints: "1",
			});

			const facts = await pool.query<{
				event_type: string;
				event_count: string;
				platform_verified: boolean;
			}>(
				`
        SELECT event_type, event_count::text, platform_verified
        FROM deterministic_event_facts
        ORDER BY event_type ASC
      `,
			);
			assert.deepEqual(facts.rows, [
				{
					event_type: "impression",
					event_count: "12",
					platform_verified: true,
				},
				{ event_type: "view", event_count: "3", platform_verified: true },
			]);

			const aggregates = await pool.query<{
				attribution_family: string;
				event_type: string;
				aggregate_count: string;
				platform_verified: boolean;
				verification_status: string;
			}>(
				`
        SELECT
          attribution_family,
          event_type,
          aggregate_count::text,
          platform_verified,
          verification_status
        FROM meta_ads_deterministic_attribution_aggregates
        ORDER BY attribution_family ASC
      `,
			);
			assert.deepEqual(aggregates.rows, [
				{
					attribution_family: "deterministic_impressions",
					event_type: "impression",
					aggregate_count: "12",
					platform_verified: true,
					verification_status: "verified",
				},
				{
					attribution_family: "deterministic_views",
					event_type: "view",
					aggregate_count: "3",
					platform_verified: true,
					verification_status: "verified",
				},
			]);

			const source = await pool.query<{
				api_endpoint: string | null;
				api_request_timestamp_utc: Date | null;
				api_account_id: string | null;
				api_request_id: string | null;
			}>(
				`
        SELECT
          api_endpoint,
          api_request_timestamp_utc,
          api_account_id,
          api_request_id
        FROM deterministic_event_sources
      `,
			);
			assert.equal(source.rows[0].api_endpoint, "insights");
			assert.ok(source.rows[0].api_request_timestamp_utc);
			assert.equal(source.rows[0].api_account_id, "123456789");
			assert.equal(source.rows[0].api_request_id, "trace-123");

			const rawPayload = await pool.query<{
				raw_payload: {
					impressions: string;
					video_play_actions: Array<{ action_type: string; value: string }>;
				};
			}>(
				`
        SELECT raw_payload
        FROM raw_deterministic_events
        WHERE event_type = 'view'
      `,
			);
			assert.deepEqual(rawPayload.rows[0].raw_payload, {
				account_id: "123456789",
				account_name: "Meta Account",
				campaign_id: "campaign-1",
				campaign_name: "Campaign 1",
				adset_id: "adset-1",
				adset_name: "Ad Set 1",
				ad_id: "ad-1",
				ad_name: "Ad 1",
				date_start: "2026-05-26",
				date_stop: "2026-05-26",
				impressions: "12",
				video_play_actions: [{ action_type: "video_view", value: "3" }],
			});

			const aggregateMetadata = await pool.query<{
				raw_record_metadata: {
					rawTable: string;
					rawEventId: number;
					apiVersion: string;
					apiEndpoint: string;
					apiAccountId: string;
					apiRequestTimestampUtc: string;
					requestId: string;
				};
			}>(
				`
        SELECT raw_record_metadata
        FROM meta_ads_deterministic_attribution_aggregates
        WHERE event_type = 'view'
      `,
			);
			assert.equal(
				aggregateMetadata.rows[0].raw_record_metadata.rawTable,
				"raw_deterministic_events",
			);
			assert.equal(
				aggregateMetadata.rows[0].raw_record_metadata.apiEndpoint,
				"insights",
			);
			assert.equal(
				aggregateMetadata.rows[0].raw_record_metadata.apiAccountId,
				"123456789",
			);
			assert.equal(
				aggregateMetadata.rows[0].raw_record_metadata.requestId,
				"trace-123",
			);
			assert.ok(aggregateMetadata.rows[0].raw_record_metadata.rawEventId);
			assert.ok(
				aggregateMetadata.rows[0].raw_record_metadata.apiRequestTimestampUtc,
			);

			const quarantine = await pool.query<{
				reason_code: string;
				platform: string;
				evidence_origin: string | null;
			}>(
				`
        SELECT reason_code, platform, evidence_origin
        FROM deterministic_event_evidence_quarantine
      `,
			);
			assert.deepEqual(quarantine.rows, [
				{
					reason_code: "missing_platform_entity",
					platform: "meta_ads",
					evidence_origin: "api",
				},
			]);

			const secondRun = await runMetaDeterministicSync({
				now: new Date("2026-05-27T12:00:00Z"),
				triggerSource: "test",
			});
			assert.equal(secondRun.succeededJobs, 1);
			assert.equal(secondRun.recordsReceived, 2);
			assert.deepEqual(await loadDeterministicCounts(), {
				jobs: "1",
				rawRows: "2",
				facts: "2",
				aggregates: "2",
				verifications: "2",
				quarantine: "2",
				checkpoints: "1",
			});
		} finally {
			globalThis.fetch = originalFetch;
		}
	},
);

test(
	"Meta deterministic sync quarantines rows when API provenance is incomplete",
	{ concurrency: false },
	async () => {
		await resetE2EDatabase();
		await seedMetaConnection({ adAccountId: "123456789", enabled: true });

		const originalFetch = globalThis.fetch;
		globalThis.fetch = async () =>
			new Response(
				JSON.stringify({
					data: [
						{
							account_id: "123456789",
							campaign_id: "campaign-1",
							ad_id: "ad-1",
							date_start: "2026-05-26",
							date_stop: "2026-05-26",
							impressions: "8",
							video_play_actions: [{ action_type: "video_view", value: "5" }],
							video_thruplay_watched_actions: [
								{ action_type: "video_thruplay_watched", value: "2" },
							],
						},
					],
				}),
				{
					status: 200,
					headers: { "content-type": "application/json" },
				},
			);

		try {
			const result = await runMetaDeterministicSync({
				now: new Date("2026-05-27T12:00:00Z"),
				triggerSource: "test",
			});

			assert.equal(result.succeededJobs, 1);
			assert.equal(result.failedJobs, 0);
			assert.equal(result.recordsReceived, 2);
			assert.equal(result.rawRowsFetched, 1);
			assert.equal(result.rawRowsUpserted, 0);
			assert.equal(result.aggregateRowsUpserted, 0);
			assert.deepEqual(await loadDeterministicCounts(), {
				jobs: "1",
				rawRows: "0",
				facts: "0",
				aggregates: "0",
				verifications: "0",
				quarantine: "2",
				checkpoints: "1",
			});

			const quarantine = await pool.query<{
				event_type: string;
				reason_code: string;
				source_id: number | null;
				source_metadata: { requestId: string | null };
				raw_payload: { impressions: string };
			}>(
				`
        SELECT event_type, reason_code, source_id, source_metadata, raw_payload
        FROM deterministic_event_evidence_quarantine
        ORDER BY event_type ASC
      `,
			);
			assert.deepEqual(
				quarantine.rows.map((row) => ({
					event_type: row.event_type,
					reason_code: row.reason_code,
					source_id: row.source_id,
					requestId: row.source_metadata.requestId,
					impressions: row.raw_payload.impressions,
				})),
				[
					{
						event_type: "impression",
						reason_code: "missing_api_request_id",
						source_id: null,
						requestId: null,
						impressions: "8",
					},
					{
						event_type: "view",
						reason_code: "missing_api_request_id",
						source_id: null,
						requestId: null,
						impressions: "8",
					},
				],
			);

			const sourceCount = await pool.query<{ count: string }>(
				"SELECT count(*)::text AS count FROM deterministic_event_sources",
			);
			assert.equal(sourceCount.rows[0].count, "0");
		} finally {
			globalThis.fetch = originalFetch;
		}
	},
);

test(
	"platform verification constraints reject non-Meta API evidence",
	{ concurrency: false },
	async () => {
		await resetE2EDatabase();

		const source = await pool.query<{ id: number }>(
			`
      INSERT INTO deterministic_event_sources (
        source_key,
        platform,
        account_id,
        evidence_origin,
        source_type
      )
      VALUES ('manual-source', 'meta_ads', '123456789', 'manual_import', 'platform_export')
      RETURNING id
    `,
		);

		await assert.rejects(
			pool.query(
				`
        INSERT INTO raw_deterministic_events (
          source_id,
          platform,
          account_id,
          campaign_id,
          event_type,
          event_date,
          event_count,
          evidence_origin,
          platform_verified,
          dedupe_key,
          raw_payload
        )
        VALUES ($1, 'meta_ads', '123456789', 'campaign-1', 'impression', '2026-05-26', 1, 'manual_import', true, 'manual-bad', '{}'::jsonb)
      `,
				[source.rows[0].id],
			),
			/raw_deterministic_events_api_verified_chk/,
		);
	},
);

test.after(async () => {
	await pool.end();
});
