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

const originalMetaAdsAdAccountId = process.env.META_ADS_AD_ACCOUNT_ID;
const originalMetaAdsEncryptionKey = process.env.META_ADS_ENCRYPTION_KEY;
const originalMetaAdsMetadataAccessToken =
	process.env.META_ADS_METADATA_ACCESS_TOKEN;

test.beforeEach(async () => {
	if (originalMetaAdsAdAccountId === undefined) {
		Reflect.deleteProperty(process.env, "META_ADS_AD_ACCOUNT_ID");
	} else {
		process.env.META_ADS_AD_ACCOUNT_ID = originalMetaAdsAdAccountId;
	}

	if (originalMetaAdsEncryptionKey === undefined) {
		Reflect.deleteProperty(process.env, "META_ADS_ENCRYPTION_KEY");
	} else {
		process.env.META_ADS_ENCRYPTION_KEY = originalMetaAdsEncryptionKey;
	}

	if (originalMetaAdsMetadataAccessToken === undefined) {
		Reflect.deleteProperty(process.env, "META_ADS_METADATA_ACCESS_TOKEN");
	} else {
		process.env.META_ADS_METADATA_ACCESS_TOKEN =
			originalMetaAdsMetadataAccessToken;
	}

	await resetE2EDatabase();
});

test.after(async () => {
	await resetE2EDatabase();
	await pool.end();
});

async function captureStructuredLogs<T>(callback: () => T | Promise<T>): Promise<{
	entries: Array<Record<string, unknown>>;
	result: T;
}> {
	const stdoutChunks: string[] = [];
	const originalStdoutWrite = process.stdout.write.bind(process.stdout);

	process.stdout.write = ((chunk: string | Uint8Array) => {
		stdoutChunks.push(
			typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"),
		);
		return true;
	}) as typeof process.stdout.write;

	try {
		const result = await callback();
		const entries = stdoutChunks
			.join("")
			.trim()
			.split("\n")
			.map((line) => line.trim())
			.filter((line) => line.startsWith("{") && line.endsWith("}"))
			.map((line) => JSON.parse(line) as Record<string, unknown>);

		return { entries, result };
	} finally {
		process.stdout.write = originalStdoutWrite as typeof process.stdout.write;
	}
}

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

test("resolveMetaMetadata can read campaign and ad set names with a runtime Meta token", async () => {
	Reflect.deleteProperty(process.env, "META_ADS_ENCRYPTION_KEY");
	process.env.META_ADS_AD_ACCOUNT_ID = "act_123456789";
	process.env.META_ADS_METADATA_ACCESS_TOKEN = "runtime-meta-token";

	const originalFetch = globalThis.fetch;
	const fetchUrls: string[] = [];

	globalThis.fetch = (async (url: string | URL | Request) => {
		const requestUrl =
			typeof url === "string" ? new URL(url) : new URL(url.toString());
		fetchUrls.push(requestUrl.toString());

		assert.equal(requestUrl.searchParams.get("access_token"), "runtime-meta-token");
		assert.equal(requestUrl.searchParams.get("fields"), "id,name,effective_status,status");

		return new Response(
			JSON.stringify({
				"222": {
					id: "222",
					name: "Runtime Campaign",
					effective_status: "ACTIVE",
				},
				"444": {
					id: "444",
					name: "Runtime Ad Set",
					status: "PAUSED",
				},
			}),
			{ status: 200 },
		);
	}) as typeof globalThis.fetch;

	try {
		const result = await resolveMetaMetadata(
			[
				{
					adAccountId: "123456789",
					objectType: "campaign",
					objectIds: ["222"],
				},
				{
					adAccountId: "act_123456789",
					objectType: "adset",
					objectIds: ["444"],
				},
			],
			{ now: new Date("2026-06-02T16:00:00.000Z") },
		);

		assert.equal(fetchUrls.length, 2);
		assert.deepEqual(
			result.resolved.map((row) => ({
				objectType: row.objectType,
				objectId: row.objectId,
				objectName: row.objectName,
				status: row.status,
				source: row.source,
			})),
			[
				{
					objectType: "campaign",
					objectId: "222",
					objectName: "Runtime Campaign",
					status: "ACTIVE",
					source: "meta_api",
				},
				{
					objectType: "adset",
					objectId: "444",
					objectName: "Runtime Ad Set",
					status: "PAUSED",
					source: "meta_api",
				},
			],
		);
		assert.deepEqual(result.unresolved, []);
	} finally {
		globalThis.fetch = originalFetch;
	}
});

test("resolveMetaMetadata logs missing runtime config and leaves raw-id fallback unresolved", async () => {
	Reflect.deleteProperty(process.env, "META_ADS_ENCRYPTION_KEY");
	Reflect.deleteProperty(process.env, "META_ADS_METADATA_ACCESS_TOKEN");
	Reflect.deleteProperty(process.env, "META_ADS_AD_ACCOUNT_ID");

	const { entries, result } = await captureStructuredLogs(() =>
		resolveMetaMetadata([
			{
				adAccountId: "act_987654321",
				objectType: "campaign",
				objectIds: ["555"],
			},
		]),
	);

	assert.deepEqual(result.resolved, []);
	assert.deepEqual(result.unresolved, [
		{
			adAccountId: "987654321",
			objectType: "campaign",
			objectId: "555",
			reason: "missing_connection",
		},
	]);

	const diagnostic = entries.find(
		(entry) => entry.event === "meta_metadata_runtime_config_diagnostic",
	);
	assert.deepEqual(
		{
			severity: diagnostic?.severity,
			platform: diagnostic?.platform,
			resolutionScope: diagnostic?.resolutionScope,
			adAccountIds: diagnostic?.adAccountIds,
			missingConfigKeys: diagnostic?.missingConfigKeys,
			fallback: diagnostic?.fallback,
		},
		{
			severity: "WARNING",
			platform: "meta_ads",
			resolutionScope: "campaign_adset_metadata",
			adAccountIds: ["987654321"],
			missingConfigKeys: [
				"META_ADS_ENCRYPTION_KEY",
				"META_ADS_METADATA_ACCESS_TOKEN",
			],
			fallback: "raw_id",
		},
	);
});
