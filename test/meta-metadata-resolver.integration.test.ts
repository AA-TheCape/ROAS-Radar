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

async function captureStructuredLogs<T>(
	callback: () => T | Promise<T>,
): Promise<{
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

test("resolveMetaMetadata returns a fresh cache hit without calling the Meta API", async () => {
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
        '2026-06-02T14:00:00.000Z'
      )
    `,
	);

	const result = await resolveMetaMetadata(
		[
			{
				adAccountId: "act_123456789",
				objectType: "campaign",
				objectIds: ["111"],
			},
		],
		{
			now: new Date("2026-06-02T15:00:00.000Z"),
			apiLookup: async () => {
				throw new Error("Meta API should not be called for a fresh cache hit");
			},
		},
	);

	assert.deepEqual(result.unresolved, []);
	assert.deepEqual(
		result.resolved.map((row) => ({
			objectId: row.objectId,
			objectName: row.objectName,
			source: row.source,
			status: row.status,
			lastFetchedAt: row.lastFetchedAt,
		})),
		[
			{
				objectId: "111",
				objectName: "Cached Campaign",
				source: "cache",
				status: "ACTIVE",
				lastFetchedAt: "2026-06-02T14:00:00.000Z",
			},
		],
	);
});

test("resolveMetaMetadata fetches and caches a cache miss from the Meta API", async () => {
	const lookupCalls: Array<{ adAccountId: string; objectIds: string[] }> = [];
	const result = await resolveMetaMetadata(
		[
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

				return new Map([
					[
						"444",
						{
							id: "444",
							name: " Meta   API Ad Set ",
							status: "PAUSED",
							objectType: "adset",
						},
					],
				]);
			},
		},
	);

	assert.deepEqual(lookupCalls, [
		{
			adAccountId: "123456789",
			objectIds: ["444"],
		},
	]);
	assert.deepEqual(result.unresolved, []);
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
				objectType: "adset",
				objectId: "444",
				objectName: "Meta API Ad Set",
				source: "meta_api",
				status: "PAUSED",
			},
		],
	);

	const cachedRows = await pool.query<{
		object_type: string;
		object_id: string;
		object_name: string | null;
		status: string | null;
		last_fetched_at: Date | null;
	}>(
		`
      SELECT object_type, object_id, object_name, status, last_fetched_at
      FROM meta_ads_metadata_cache
      WHERE ad_account_id = '123456789'
    `,
	);

	assert.deepEqual(
		cachedRows.rows.map((row) => ({
			objectType: row.object_type,
			objectId: row.object_id,
			objectName: row.object_name,
			status: row.status,
			lastFetchedAt: row.last_fetched_at?.toISOString() ?? null,
		})),
		[
			{
				objectType: "adset",
				objectId: "444",
				objectName: "Meta API Ad Set",
				status: "PAUSED",
				lastFetchedAt: "2026-06-02T15:00:00.000Z",
			},
		],
	);
});

test("resolveMetaMetadata rejects wrong Meta object types and does not cache them as the requested type", async () => {
	const lookupCalls: Array<{
		adAccountId: string;
		objectType: "campaign" | "adset";
		objectIds: string[];
	}> = [];
	const result = await resolveMetaMetadata(
		[
			{
				adAccountId: "123456789",
				objectType: "campaign",
				objectIds: ["777"],
			},
			{
				adAccountId: "123456789",
				objectType: "adset",
				objectIds: ["777"],
			},
		],
		{
			now: new Date("2026-06-02T15:00:00.000Z"),
			apiLookup: async ({ adAccountId, objectType, objectIds }) => {
				lookupCalls.push({ adAccountId, objectType, objectIds });

				return new Map([
					[
						"777",
						{
							id: "777",
							name:
								objectType === "campaign"
									? "Wrongly Returned Ad Set"
									: "Verified Ad Set",
							status: "ACTIVE",
							objectType: "adset",
						},
					],
				]);
			},
		},
	);

	assert.deepEqual(lookupCalls, [
		{
			adAccountId: "123456789",
			objectType: "campaign",
			objectIds: ["777"],
		},
		{
			adAccountId: "123456789",
			objectType: "adset",
			objectIds: ["777"],
		},
	]);
	assert.deepEqual(
		result.resolved.map((row) => ({
			objectType: row.objectType,
			objectId: row.objectId,
			objectName: row.objectName,
			source: row.source,
		})),
		[
			{
				objectType: "adset",
				objectId: "777",
				objectName: "Verified Ad Set",
				source: "meta_api",
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
				objectId: "777",
				reason: "meta_api_not_found",
			},
		],
	);

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
      ORDER BY object_type ASC
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
				objectId: "777",
				objectName: "Verified Ad Set",
				status: "ACTIVE",
				lastFetchedAt: "2026-06-02T15:00:00.000Z",
				lookupFailedAt: null,
			},
			{
				objectType: "campaign",
				objectId: "777",
				objectName: null,
				status: null,
				lastFetchedAt: null,
				lookupFailedAt: "2026-06-02T15:00:00.000Z",
			},
		],
	);
});

test("resolveMetaMetadata does not log unresolved alternate scopes when the id resolved as an accepted Meta object type", async () => {
	const { entries, result } = await captureStructuredLogs(() =>
		resolveMetaMetadata(
			[
				{
					adAccountId: "123456789",
					objectType: "campaign",
					objectIds: ["777"],
				},
				{
					adAccountId: "123456789",
					objectType: "adset",
					objectIds: ["777", "888"],
				},
			],
			{
				now: new Date("2026-06-02T15:00:00.000Z"),
				apiLookup: async ({ objectType, objectIds }) =>
					new Map(
						objectIds
							.filter((objectId) => objectType === "campaign" && objectId === "777")
							.map((objectId) => [
								objectId,
								{
									id: objectId,
									name: "Resolved Campaign",
									status: "ACTIVE",
									objectType: "campaign" as const,
								},
							]),
					),
			},
		),
	);

	assert.deepEqual(
		result.resolved.map((entry) => ({
			objectType: entry.objectType,
			objectId: entry.objectId,
		})),
		[
			{
				objectType: "campaign",
				objectId: "777",
			},
		],
	);
	assert.deepEqual(
		result.unresolved.map((entry) => ({
			objectType: entry.objectType,
			objectId: entry.objectId,
			reason: entry.reason,
		})),
		[
			{
				objectType: "adset",
				objectId: "777",
				reason: "meta_api_not_found",
			},
			{
				objectType: "adset",
				objectId: "888",
				reason: "meta_api_not_found",
			},
		],
	);

	const summary = entries.find(
		(entry) => entry.event === "meta_metadata_lookup_summary",
	);

	assert.deepEqual(
		{
			apiLookupObjectCount: summary?.apiLookupObjectCount,
			apiResolvedCount: summary?.apiResolvedCount,
			apiNotFoundCount: summary?.apiNotFoundCount,
			unresolvedCount: summary?.unresolvedCount,
			unresolvedEntityIds: summary?.unresolvedEntityIds,
			unresolvedObjectScopes: summary?.unresolvedObjectScopes,
			unresolvedReasons: summary?.unresolvedReasons,
		},
		{
			apiLookupObjectCount: 3,
			apiResolvedCount: 1,
			apiNotFoundCount: 2,
			unresolvedCount: 1,
			unresolvedEntityIds: ["888"],
			unresolvedObjectScopes: [
				{
					objectId: "888",
					objectType: "adset",
					reason: "meta_api_not_found",
				},
			],
			unresolvedReasons: {
				meta_api_not_found: 1,
			},
		},
	);
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
					{
						id: string;
						name: string | null;
						status: string | null;
						objectType: "campaign" | "adset";
					}
				>();

				if (objectIds.includes("222")) {
					rows.set("222", {
						id: "222",
						name: " API Campaign ",
						status: "PAUSED",
						objectType: "campaign",
					});
				}

				if (objectIds.includes("444")) {
					rows.set("444", {
						id: "444",
						name: "US Prospecting Ad Set",
						status: null,
						objectType: "adset",
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

test("resolveMetaMetadata emits lookup summary logs without connection secrets", async () => {
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
        'Cached Campaign',
        'ACTIVE',
        '2026-06-02T14:00:00.000Z'
      )
    `,
	);

	const { entries, result } = await captureStructuredLogs(() =>
		resolveMetaMetadata(
			[
				{
					adAccountId: "act_123456789",
					objectType: "campaign",
					objectIds: ["111", "222", "333", "bad-id"],
				},
			],
			{
				now: new Date("2026-06-02T15:00:00.000Z"),
				apiLookup: async ({ objectIds }) =>
					new Map(
						objectIds
							.filter((objectId) => objectId === "222")
							.map((objectId) => [
								objectId,
								{
									id: objectId,
									name: "Resolved From API",
									status: "ACTIVE",
									objectType: "campaign",
								},
							]),
					),
			},
		),
	);

	assert.equal(result.resolved.length, 2);
	assert.equal(result.unresolved.length, 2);

	const summary = entries.find(
		(entry) => entry.event === "meta_metadata_lookup_summary",
	);

	assert.deepEqual(
		{
			severity: summary?.severity,
			platform: summary?.platform,
			resolutionScope: summary?.resolutionScope,
			requestedCount: summary?.requestedCount,
			normalizedRequestCount: summary?.normalizedRequestCount,
			invalidIdCount: summary?.invalidIdCount,
			cacheHitCount: summary?.cacheHitCount,
			cacheMissCount: summary?.cacheMissCount,
			apiRequestCount: summary?.apiRequestCount,
			apiLookupObjectCount: summary?.apiLookupObjectCount,
			apiResolvedCount: summary?.apiResolvedCount,
			apiNotFoundCount: summary?.apiNotFoundCount,
			apiFailureCount: summary?.apiFailureCount,
			unresolvedCount: summary?.unresolvedCount,
			unresolvedEntityIds: summary?.unresolvedEntityIds,
			unresolvedReasons: summary?.unresolvedReasons,
		},
		{
			severity: "INFO",
			platform: "meta_ads",
			resolutionScope: "campaign_adset_metadata",
			requestedCount: 4,
			normalizedRequestCount: 3,
			invalidIdCount: 1,
			cacheHitCount: 1,
			cacheMissCount: 2,
			apiRequestCount: 1,
			apiLookupObjectCount: 2,
			apiResolvedCount: 1,
			apiNotFoundCount: 1,
			apiFailureCount: 0,
			unresolvedCount: 2,
			unresolvedEntityIds: ["bad-id", "333"],
			unresolvedReasons: {
				invalid_id: 1,
				meta_api_not_found: 1,
			},
		},
	);

	const serializedSummary = JSON.stringify(summary);
	assert.equal(serializedSummary.includes("access_token"), false);
	assert.equal(serializedSummary.includes("runtime-meta-token"), false);
	assert.equal(serializedSummary.includes("META_ADS_METADATA_ACCESS_TOKEN"), false);
});

test("resolveMetaMetadata refreshes stale cached names after the cache TTL", async () => {
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
        'Old Campaign Name',
        'PAUSED',
        '2026-05-01T12:00:00.000Z'
      )
    `,
	);

	const lookupCalls: Array<{ adAccountId: string; objectIds: string[] }> = [];
	const result = await resolveMetaMetadata(
		[
			{
				adAccountId: "123456789",
				objectType: "campaign",
				objectIds: ["111"],
			},
		],
		{
			now: new Date("2026-06-02T15:00:00.000Z"),
			cacheTtlMs: 7 * 24 * 60 * 60 * 1000,
			apiLookup: async ({ adAccountId, objectIds }) => {
				lookupCalls.push({ adAccountId, objectIds });

				return new Map([
					[
						"111",
						{
							id: "111",
							name: "New Campaign Name",
							status: "ACTIVE",
							objectType: "campaign",
						},
					],
				]);
			},
		},
	);

	assert.deepEqual(lookupCalls, [
		{
			adAccountId: "123456789",
			objectIds: ["111"],
		},
	]);
	assert.deepEqual(
		result.resolved.map((row) => ({
			objectId: row.objectId,
			objectName: row.objectName,
			source: row.source,
			status: row.status,
		})),
		[
			{
				objectId: "111",
				objectName: "New Campaign Name",
				source: "meta_api",
				status: "ACTIVE",
			},
		],
	);

	const cachedRow = await pool.query<{
		object_name: string;
		status: string | null;
		last_fetched_at: Date | null;
	}>(
		`
      SELECT object_name, status, last_fetched_at
      FROM meta_ads_metadata_cache
      WHERE ad_account_id = '123456789'
        AND object_type = 'campaign'
        AND object_id = '111'
    `,
	);

	assert.deepEqual(
		cachedRow.rows.map((row) => ({
			objectName: row.object_name,
			status: row.status,
			lastFetchedAt: row.last_fetched_at?.toISOString() ?? null,
		})),
		[
			{
				objectName: "New Campaign Name",
				status: "ACTIVE",
				lastFetchedAt: "2026-06-02T15:00:00.000Z",
			},
		],
	);
});

test("resolveMetaMetadata keeps stale cached names when a refresh fails", async () => {
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
        'Fallback Campaign',
        'ACTIVE',
        '2026-05-01T12:00:00.000Z'
      )
    `,
	);

	const result = await resolveMetaMetadata(
		[
			{
				adAccountId: "123456789",
				objectType: "campaign",
				objectIds: ["111", "222"],
			},
		],
		{
			now: new Date("2026-06-02T15:00:00.000Z"),
			cacheTtlMs: 7 * 24 * 60 * 60 * 1000,
			apiLookup: async () => {
				throw new Error("Meta API quota exceeded");
			},
		},
	);

	assert.deepEqual(
		result.resolved.map((row) => ({
			objectId: row.objectId,
			objectName: row.objectName,
			source: row.source,
		})),
		[
			{
				objectId: "111",
				objectName: "Fallback Campaign",
				source: "cache",
			},
		],
	);
	assert.deepEqual(
		result.unresolved.map((row) => ({
			objectId: row.objectId,
			reason: row.reason,
			error: row.error,
		})),
		[
			{
				objectId: "222",
				reason: "meta_api_error",
				error: "Meta API quota exceeded",
			},
		],
	);
});

test("resolveMetaMetadata does not refetch recently unresolved ids inside retry window", async () => {
	await pool.query(
		`
      INSERT INTO meta_ads_metadata_cache (
        ad_account_id,
        object_type,
        object_id,
        lookup_failed_at
      )
      VALUES (
        '123456789',
        'campaign',
        '333',
        '2026-06-02T14:45:00.000Z'
      )
    `,
	);

	const lookupCalls: Array<{ adAccountId: string; objectIds: string[] }> = [];
	const result = await resolveMetaMetadata(
		[
			{
				adAccountId: "123456789",
				objectType: "campaign",
				objectIds: ["333", "444"],
			},
		],
		{
			now: new Date("2026-06-02T15:00:00.000Z"),
			retryWindowMs: 60 * 60 * 1000,
			apiLookup: async ({ adAccountId, objectIds }) => {
				lookupCalls.push({ adAccountId, objectIds });

				return new Map([
					[
						"444",
						{
							id: "444",
							name: "Resolvable Campaign",
							status: null,
							objectType: "campaign",
						},
					],
				]);
			},
		},
	);

	assert.deepEqual(lookupCalls, [
		{
			adAccountId: "123456789",
			objectIds: ["444"],
		},
	]);
	assert.deepEqual(
		result.resolved.map((row) => ({
			objectId: row.objectId,
			objectName: row.objectName,
			source: row.source,
		})),
		[
			{
				objectId: "444",
				objectName: "Resolvable Campaign",
				source: "meta_api",
			},
		],
	);
	assert.deepEqual(
		result.unresolved.map((row) => ({
			objectId: row.objectId,
			reason: row.reason,
		})),
		[
			{
				objectId: "333",
				reason: "meta_api_not_found",
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

		assert.equal(
			requestUrl.searchParams.get("access_token"),
			"runtime-meta-token",
		);
		assert.equal(
			requestUrl.searchParams.get("fields"),
			"id,name,effective_status,status",
		);
		assert.equal(requestUrl.searchParams.get("limit"), "1");

		const filtering = JSON.parse(
			requestUrl.searchParams.get("filtering") ?? "[]",
		) as Array<{ field?: string; operator?: string; value?: string[] }>;
		assert.deepEqual(filtering[0]?.field, "id");
		assert.deepEqual(filtering[0]?.operator, "IN");

		if (requestUrl.pathname.endsWith("/act_123456789/campaigns")) {
			assert.deepEqual(filtering[0]?.value, ["222"]);

			return new Response(
				JSON.stringify({
					data: [
						{
							id: "222",
							name: "Runtime Campaign",
							effective_status: "ACTIVE",
						},
					],
				}),
				{ status: 200 },
			);
		}

		if (requestUrl.pathname.endsWith("/act_123456789/adsets")) {
			assert.deepEqual(filtering[0]?.value, ["444"]);

			return new Response(
				JSON.stringify({
					data: [
						{
							id: "444",
							name: "Runtime Ad Set",
							status: "PAUSED",
						},
					],
				}),
				{ status: 200 },
			);
		}

		return new Response(JSON.stringify({ error: { message: "unexpected" } }), {
			status: 404,
		});
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
