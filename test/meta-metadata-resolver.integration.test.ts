import assert from "node:assert/strict";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test";
process.env.META_ADS_ENCRYPTION_KEY ??= "meta-encryption-key";
process.env.META_ADS_API_VERSION ??= "v20.0";

const { pool } = await import("../src/db/pool.js");
const { resolveMetaMetadata } = await import(
	"../src/modules/meta-ads/index.js"
);
const { resetE2EDatabase } = await import("./e2e-harness.js");

test.beforeEach(async () => {
	await resetE2EDatabase();
});

test.after(async () => {
	await resetE2EDatabase();
	await pool.end();
});

test("resolveMetaMetadata reads cache first, fetches missing Meta ids, and returns unresolved ids", async () => {
	await pool.query(
		`
      INSERT INTO meta_ads_metadata_cache (
        ad_account_id,
        object_type,
        object_id,
        object_name,
        status,
        last_fetched_at
      )
      VALUES (
        '123456789',
        'campaign',
        '111',
        ' Cached   Campaign ',
        'ACTIVE',
        '2026-06-01T12:00:00.000Z'
      )
    `,
	);

	const lookupCalls: Array<{ adAccountId: string; objectIds: string[] }> = [];

	const result = await resolveMetaMetadata(
		[
			{
				adAccountId: "act_123456789",
				objectType: "campaign",
				objectIds: ["111", "222", "333", "bad-id"],
			},
			{
				adAccountId: "123456789",
				objectType: "adset",
				objectIds: ["444"],
			},
		],
		{
			now: new Date("2026-06-02T15:00:00.000Z"),
			apiLookup: async ({ adAccountId, objectIds }) => {
				lookupCalls.push({ adAccountId, objectIds });

				const rows = new Map<
					string,
					{ id: string; name: string | null; status: string | null }
				>();

				if (objectIds.includes("222")) {
					rows.set("222", {
						id: "222",
						name: " API Campaign ",
						status: "PAUSED",
					});
				}

				if (objectIds.includes("444")) {
					rows.set("444", {
						id: "444",
						name: "US Prospecting Ad Set",
						status: null,
					});
				}

				return rows;
			},
		},
	);

	assert.deepEqual(
		result.resolved.map((row) => ({
			objectType: row.objectType,
			objectId: row.objectId,
			objectName: row.objectName,
			source: row.source,
			status: row.status,
		})),
		[
			{
				objectType: "campaign",
				objectId: "111",
				objectName: "Cached Campaign",
				source: "cache",
				status: "ACTIVE",
			},
			{
				objectType: "campaign",
				objectId: "222",
				objectName: "API Campaign",
				source: "meta_api",
				status: "PAUSED",
			},
			{
				objectType: "adset",
				objectId: "444",
				objectName: "US Prospecting Ad Set",
				source: "meta_api",
				status: null,
			},
		],
	);

	assert.deepEqual(
		result.unresolved.map((row) => ({
			objectType: row.objectType,
			objectId: row.objectId,
			reason: row.reason,
		})),
		[
			{
				objectType: "campaign",
				objectId: "bad-id",
				reason: "invalid_id",
			},
			{
				objectType: "campaign",
				objectId: "333",
				reason: "meta_api_not_found",
			},
		],
	);

	assert.deepEqual(lookupCalls, [
		{
			adAccountId: "123456789",
			objectIds: ["222", "333"],
		},
		{
			adAccountId: "123456789",
			objectIds: ["444"],
		},
	]);

	const cachedRows = await pool.query<{
		object_type: string;
		object_id: string;
		object_name: string | null;
		status: string | null;
		last_fetched_at: Date | null;
		lookup_failed_at: Date | null;
	}>(
		`
      SELECT object_type, object_id, object_name, status, last_fetched_at, lookup_failed_at
      FROM meta_ads_metadata_cache
      WHERE ad_account_id = '123456789'
      ORDER BY object_type ASC, object_id ASC
    `,
	);

	assert.deepEqual(
		cachedRows.rows.map((row) => ({
			objectType: row.object_type,
			objectId: row.object_id,
			objectName: row.object_name,
			status: row.status,
			lastFetchedAt: row.last_fetched_at?.toISOString() ?? null,
			lookupFailedAt: row.lookup_failed_at?.toISOString() ?? null,
		})),
		[
			{
				objectType: "adset",
				objectId: "444",
				objectName: "US Prospecting Ad Set",
				status: null,
				lastFetchedAt: "2026-06-02T15:00:00.000Z",
				lookupFailedAt: null,
			},
			{
				objectType: "campaign",
				objectId: "111",
				objectName: " Cached   Campaign ",
				status: "ACTIVE",
				lastFetchedAt: "2026-06-01T12:00:00.000Z",
				lookupFailedAt: null,
			},
			{
				objectType: "campaign",
				objectId: "222",
				objectName: "API Campaign",
				status: "PAUSED",
				lastFetchedAt: "2026-06-02T15:00:00.000Z",
				lookupFailedAt: null,
			},
			{
				objectType: "campaign",
				objectId: "333",
				objectName: null,
				status: null,
				lastFetchedAt: null,
				lookupFailedAt: "2026-06-02T15:00:00.000Z",
			},
		],
	);
});
