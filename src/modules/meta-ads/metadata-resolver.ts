import { env } from "../../config/env.js";
import { query } from "../../db/pool.js";
import {
	emitMetaMetadataLookupSummaryLog,
	logWarning,
} from "../../observability/index.js";

const META_GRAPH_BASE_URL = "https://graph.facebook.com";
const META_RESOLVABLE_OBJECT_TYPES = ["campaign", "adset"] as const;
const META_OBJECT_FIELDS = "id,name,effective_status,status";
const DEFAULT_META_METADATA_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_META_METADATA_RETRY_WINDOW_MS = 60 * 60 * 1000;

export type MetaMetadataObjectType =
	(typeof META_RESOLVABLE_OBJECT_TYPES)[number];

export type MetaMetadataResolutionRequest = {
	adAccountId: string;
	objectType: MetaMetadataObjectType;
	objectIds: string[];
};

export type MetaMetadataResolvedObject = {
	adAccountId: string;
	objectType: MetaMetadataObjectType;
	objectId: string;
	objectName: string;
	status: string | null;
	source: "cache" | "meta_api";
	lastFetchedAt: string | null;
};

export type MetaMetadataUnresolvedObject = {
	adAccountId: string;
	objectType: MetaMetadataObjectType;
	objectId: string;
	reason:
		| "invalid_id"
		| "missing_connection"
		| "meta_api_not_found"
		| "meta_api_error";
	error?: string;
};

export type MetaMetadataResolutionResult = {
	resolved: MetaMetadataResolvedObject[];
	unresolved: MetaMetadataUnresolvedObject[];
};

export type MetaMetadataApiLookup = (input: {
	adAccountId: string;
	accessToken: string;
	objectType: MetaMetadataObjectType;
	objectIds: string[];
}) => Promise<Map<string, MetaMetadataApiObject>>;

type MetaMetadataApiObject = {
	id: string;
	name: string | null;
	status: string | null;
	objectType: MetaMetadataObjectType;
};

type MetaMetadataCacheRow = {
	ad_account_id: string;
	object_type: MetaMetadataObjectType;
	object_id: string;
	object_name: string | null;
	status: string | null;
	last_fetched_at: Date | null;
	lookup_failed_at: Date | null;
};

type MetaMetadataCachePolicy = {
	freshResolved: Map<string, MetaMetadataResolvedObject>;
	staleResolved: Map<string, MetaMetadataResolvedObject>;
	recentFailures: Map<string, MetaMetadataUnresolvedObject>;
};

type MetaMetadataConnectionRow = {
	ad_account_id: string;
	access_token: string;
	source: "database" | "runtime_config";
};

type MetaGraphObjectResponse = {
	id?: string;
	name?: string;
	effective_status?: string;
	status?: string;
	error?: {
		message?: string;
	};
};

type MetaGraphCollectionResponse = {
	data?: MetaGraphObjectResponse[];
	error?: {
		message?: string;
	};
};

const emittedRuntimeDiagnosticKeys = new Set<string>();

function normalizeString(value: string | null | undefined): string | null {
	if (typeof value !== "string") {
		return null;
	}

	const trimmed = value.trim();
	return trimmed ? trimmed : null;
}

function collapseWhitespace(value: string | null | undefined): string | null {
	const normalized = normalizeString(value);
	return normalized ? normalized.replace(/\s+/g, " ") : null;
}

function normalizeObjectIds(objectIds: string[]): {
	validIds: string[];
	invalidIds: string[];
} {
	const validIds = new Set<string>();
	const invalidIds: string[] = [];

	for (const objectId of objectIds) {
		const normalized = normalizeString(objectId);

		if (!normalized) {
			continue;
		}

		if (!/^\d+$/.test(normalized)) {
			invalidIds.push(normalized);
			continue;
		}

		validIds.add(normalized);
	}

	return {
		validIds: [...validIds],
		invalidIds: [...new Set(invalidIds)],
	};
}

function normalizeAdAccountId(adAccountId: string): string | null {
	const normalized = normalizeString(adAccountId);

	if (!normalized) {
		return null;
	}

	return normalized.startsWith("act_") ? normalized.slice(4) : normalized;
}

function buildResolutionKey(
	adAccountId: string,
	objectType: MetaMetadataObjectType,
	objectId: string,
): string {
	return `${adAccountId}\u0000${objectType}\u0000${objectId}`;
}

function isWithinWindow(
	timestamp: Date | null,
	now: Date,
	windowMs: number,
): boolean {
	if (!timestamp) {
		return false;
	}

	const ageMs = now.getTime() - timestamp.getTime();
	return ageMs >= 0 && ageMs <= windowMs;
}

async function loadMetaMetadataCachePolicy(
	requests: Array<{
		adAccountId: string;
		objectType: MetaMetadataObjectType;
		objectIds: string[];
	}>,
	now: Date,
	cacheTtlMs: number,
	retryWindowMs: number,
): Promise<MetaMetadataCachePolicy> {
	const cachePolicy: MetaMetadataCachePolicy = {
		freshResolved: new Map(),
		staleResolved: new Map(),
		recentFailures: new Map(),
	};

	if (requests.length === 0) {
		return cachePolicy;
	}

	const rows: Array<{
		adAccountId: string;
		objectType: MetaMetadataObjectType;
		objectId: string;
	}> = [];

	for (const request of requests) {
		for (const objectId of request.objectIds) {
			rows.push({
				adAccountId: request.adAccountId,
				objectType: request.objectType,
				objectId,
			});
		}
	}

	const result = await query<MetaMetadataCacheRow>(
		`
      SELECT
        c.ad_account_id,
        c.object_type,
        c.object_id,
        c.object_name,
        c.status,
        c.last_fetched_at,
        c.lookup_failed_at
      FROM meta_ads_metadata_cache c
      JOIN jsonb_to_recordset($1::jsonb) AS requested(
        ad_account_id text,
        object_type text,
        object_id text
      )
        ON requested.ad_account_id = c.ad_account_id
       AND requested.object_type = c.object_type
       AND requested.object_id = c.object_id
      WHERE (
          c.last_fetched_at IS NOT NULL
          AND c.object_name IS NOT NULL
        )
        OR c.lookup_failed_at IS NOT NULL
    `,
		[
			JSON.stringify(
				rows.map((row) => ({
					ad_account_id: row.adAccountId,
					object_type: row.objectType,
					object_id: row.objectId,
				})),
			),
		],
	);

	for (const row of result.rows) {
		const key = buildResolutionKey(
			row.ad_account_id,
			row.object_type,
			row.object_id,
		);
		const objectName = collapseWhitespace(row.object_name);

		if (objectName && row.last_fetched_at) {
			const resolved = {
				adAccountId: row.ad_account_id,
				objectType: row.object_type,
				objectId: row.object_id,
				objectName,
				status: collapseWhitespace(row.status),
				source: "cache",
				lastFetchedAt: row.last_fetched_at.toISOString(),
			} satisfies MetaMetadataResolvedObject;

			if (isWithinWindow(row.last_fetched_at, now, cacheTtlMs)) {
				cachePolicy.freshResolved.set(key, resolved);
			} else {
				cachePolicy.staleResolved.set(key, resolved);
			}
		}

		if (isWithinWindow(row.lookup_failed_at, now, retryWindowMs)) {
			cachePolicy.recentFailures.set(key, {
				adAccountId: row.ad_account_id,
				objectType: row.object_type,
				objectId: row.object_id,
				reason: "meta_api_not_found",
			});
		}
	}

	return cachePolicy;
}

async function loadActiveMetaMetadataConnections(
	accountIds: string[],
): Promise<Map<string, MetaMetadataConnectionRow>> {
	const normalizedAccountIds = [
		...new Set(
			accountIds
				.map(normalizeAdAccountId)
				.filter((value): value is string => Boolean(value)),
		),
	];

	const connections = new Map<string, MetaMetadataConnectionRow>();
	const configuredAccountId = normalizeAdAccountId(env.META_ADS_AD_ACCOUNT_ID);
	const configuredAccessToken = normalizeString(
		env.META_ADS_METADATA_ACCESS_TOKEN,
	);

	if (configuredAccountId && configuredAccessToken) {
		for (const accountId of normalizedAccountIds) {
			if (accountId === configuredAccountId) {
				connections.set(accountId, {
					ad_account_id: accountId,
					access_token: configuredAccessToken,
					source: "runtime_config",
				});
			}
		}
	}

	if (normalizedAccountIds.length === 0 || !env.META_ADS_ENCRYPTION_KEY) {
		return connections;
	}

	const result = await query<MetaMetadataConnectionRow>(
		`
      SELECT
        ad_account_id,
        pgp_sym_decrypt(access_token_encrypted, $1) AS access_token
      FROM meta_ads_connections
      WHERE status = 'active'
        AND ad_account_id = ANY($2::text[])
      ORDER BY updated_at DESC
    `,
		[env.META_ADS_ENCRYPTION_KEY, normalizedAccountIds],
	);

	for (const row of result.rows) {
		if (!connections.has(row.ad_account_id)) {
			connections.set(row.ad_account_id, {
				...row,
				source: "database",
			});
		}
	}

	return connections;
}

function emitMetaMetadataRuntimeDiagnostic(input: {
	adAccountIds: string[];
	missingConfigKeys: string[];
	message: string;
}): void {
	const key = `${input.missingConfigKeys.sort().join(",")}\u0000${input.adAccountIds
		.slice()
		.sort()
		.join(",")}`;

	if (emittedRuntimeDiagnosticKeys.has(key)) {
		return;
	}

	emittedRuntimeDiagnosticKeys.add(key);

	logWarning("meta_metadata_runtime_config_diagnostic", {
		service: process.env.K_SERVICE ?? "roas-radar",
		platform: "meta_ads",
		resolutionScope: "campaign_adset_metadata",
		adAccountIds: input.adAccountIds.slice(0, 10),
		missingConfigKeys: input.missingConfigKeys,
		fallback: "raw_id",
		message: input.message,
	});
}

async function fetchMetaObjectsByIds(input: {
	adAccountId: string;
	accessToken: string;
	objectType: MetaMetadataObjectType;
	objectIds: string[];
}): Promise<Map<string, MetaMetadataApiObject>> {
	const resolved = new Map<string, MetaMetadataApiObject>();
	const edge = input.objectType === "campaign" ? "campaigns" : "adsets";
	const scopedAccountId = `act_${input.adAccountId}`;

	for (let index = 0; index < input.objectIds.length; index += 50) {
		const chunk = input.objectIds.slice(index, index + 50);
		const url = new URL(
			`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/${scopedAccountId}/${edge}`,
		);
		url.searchParams.set("access_token", input.accessToken);
		url.searchParams.set("fields", META_OBJECT_FIELDS);
		url.searchParams.set(
			"filtering",
			JSON.stringify([{ field: "id", operator: "IN", value: chunk }]),
		);
		url.searchParams.set("limit", String(chunk.length));

		const response = await fetch(url);
		const payload = (await response.json()) as MetaGraphCollectionResponse;

		if (!response.ok) {
			const message = payload.error?.message;
			throw new Error(
				message ??
					`Meta Ads metadata lookup failed with status ${response.status}`,
			);
		}

		for (const entry of payload.data ?? []) {
			const id = normalizeString(entry?.id);
			const name = collapseWhitespace(entry?.name);

			if (!id || !name || entry?.error) {
				continue;
			}

			resolved.set(id, {
				id,
				name,
				status: collapseWhitespace(entry.effective_status ?? entry.status),
				objectType: input.objectType,
			});
		}
	}

	return resolved;
}

async function upsertSuccessfulMetaMetadataCache(
	resolutions: MetaMetadataResolvedObject[],
	fetchedAt: Date,
): Promise<void> {
	for (const resolution of resolutions) {
		if (resolution.source !== "meta_api") {
			continue;
		}

		await query(
			`
        INSERT INTO meta_ads_metadata_cache (
          ad_account_id,
          object_type,
          object_id,
          object_name,
          status,
          last_fetched_at,
          lookup_failed_at,
          updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NULL, now())
        ON CONFLICT (ad_account_id, object_type, object_id)
        DO UPDATE
        SET
          object_name = EXCLUDED.object_name,
          status = EXCLUDED.status,
          last_fetched_at = EXCLUDED.last_fetched_at,
          lookup_failed_at = NULL,
          updated_at = now()
      `,
			[
				resolution.adAccountId,
				resolution.objectType,
				resolution.objectId,
				resolution.objectName,
				resolution.status,
				fetchedAt,
			],
		);
	}
}

async function markFailedMetaMetadataLookups(
	unresolved: MetaMetadataUnresolvedObject[],
	failedAt: Date,
): Promise<void> {
	for (const failure of unresolved) {
		if (failure.reason !== "meta_api_not_found") {
			continue;
		}

		await query(
			`
        INSERT INTO meta_ads_metadata_cache (
          ad_account_id,
          object_type,
          object_id,
          lookup_failed_at,
          updated_at
        )
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (ad_account_id, object_type, object_id)
        DO UPDATE
        SET
          lookup_failed_at = EXCLUDED.lookup_failed_at,
          updated_at = now()
      `,
			[failure.adAccountId, failure.objectType, failure.objectId, failedAt],
		);
	}
}

export async function resolveMetaMetadata(
	requests: MetaMetadataResolutionRequest[],
	options: {
		now?: Date;
		apiLookup?: MetaMetadataApiLookup;
		cacheTtlMs?: number;
		retryWindowMs?: number;
	} = {},
): Promise<MetaMetadataResolutionResult> {
	const now = options.now ?? new Date();
	const cacheTtlMs = options.cacheTtlMs ?? DEFAULT_META_METADATA_CACHE_TTL_MS;
	const retryWindowMs =
		options.retryWindowMs ?? DEFAULT_META_METADATA_RETRY_WINDOW_MS;
	const normalizedRequests: Array<{
		adAccountId: string;
		objectType: MetaMetadataObjectType;
		objectIds: string[];
	}> = [];
	const unresolved: MetaMetadataUnresolvedObject[] = [];
	let requestedCount = 0;
	let invalidIdCount = 0;

	for (const request of requests) {
		const adAccountId = normalizeAdAccountId(request.adAccountId);

		if (!adAccountId) {
			continue;
		}

		const { validIds, invalidIds } = normalizeObjectIds(request.objectIds);
		requestedCount += validIds.length + invalidIds.length;
		invalidIdCount += invalidIds.length;

		for (const invalidId of invalidIds) {
			unresolved.push({
				adAccountId,
				objectType: request.objectType,
				objectId: invalidId,
				reason: "invalid_id",
			});
		}

		if (validIds.length === 0) {
			continue;
		}

		normalizedRequests.push({
			adAccountId,
			objectType: request.objectType,
			objectIds: validIds,
		});
	}

	const normalizedRequestCount = normalizedRequests.reduce(
		(total, request) => total + request.objectIds.length,
		0,
	);

	const cachePolicy = await loadMetaMetadataCachePolicy(
		normalizedRequests,
		now,
		cacheTtlMs,
		retryWindowMs,
	);
	const resolved = [...cachePolicy.freshResolved.values()];
	const missingByAccountType = new Map<
		string,
		{
			adAccountId: string;
			objectType: MetaMetadataObjectType;
			objectIds: string[];
		}
	>();
	let recentFailureCacheHitCount = 0;

	for (const request of normalizedRequests) {
		for (const objectId of request.objectIds) {
			const resolutionKey = buildResolutionKey(
				request.adAccountId,
				request.objectType,
				objectId,
			);

			if (cachePolicy.freshResolved.has(resolutionKey)) {
				continue;
			}

			const recentFailure = cachePolicy.recentFailures.get(resolutionKey);

			if (recentFailure && !cachePolicy.staleResolved.has(resolutionKey)) {
				unresolved.push(recentFailure);
				recentFailureCacheHitCount += 1;
				continue;
			}

			const groupKey = `${request.adAccountId}\u0000${request.objectType}`;
			const group = missingByAccountType.get(groupKey) ?? {
				adAccountId: request.adAccountId,
				objectType: request.objectType,
				objectIds: [],
			};

			group.objectIds.push(objectId);
			missingByAccountType.set(groupKey, group);
		}
	}

	const cacheMissCount = [...missingByAccountType.values()].reduce(
		(total, group) => total + group.objectIds.length,
		0,
	);

	if (missingByAccountType.size === 0) {
		const unresolvedReasons = summarizeUnresolvedReasons(unresolved);

		emitMetaMetadataLookupSummaryLog({
			resolutionScope: "campaign_adset_metadata",
			requestedCount,
			normalizedRequestCount,
			invalidIdCount,
			cacheHitCount: resolved.length,
			staleCacheHitCount: 0,
			recentFailureCacheHitCount,
			cacheMissCount,
			apiRequestCount: 0,
			apiLookupObjectCount: 0,
			apiResolvedCount: 0,
			apiNotFoundCount: 0,
			apiFailureCount: 0,
			missingConnectionCount: 0,
			unresolvedCount: unresolved.length,
			unresolvedEntityIds: unresolved.map((entry) => entry.objectId),
			unresolvedReasons,
		});

		return { resolved, unresolved };
	}

	const connections = options.apiLookup
		? new Map<string, MetaMetadataConnectionRow>()
		: await loadActiveMetaMetadataConnections([
				...new Set(
					[...missingByAccountType.values()].map((group) => group.adAccountId),
				),
			]);
	const fetchedAt = now;
	const apiResolved: MetaMetadataResolvedObject[] = [];
	const apiUnresolved: MetaMetadataUnresolvedObject[] = [];
	let apiRequestCount = 0;
	let apiLookupObjectCount = 0;
	let apiFailureCount = 0;
	let staleCacheHitCount = 0;

	for (const group of missingByAccountType.values()) {
		const connection = connections.get(group.adAccountId);

		if (!options.apiLookup && !connection) {
			const missingConfigKeys = [
				!env.META_ADS_ENCRYPTION_KEY ? "META_ADS_ENCRYPTION_KEY" : null,
				!env.META_ADS_METADATA_ACCESS_TOKEN
					? "META_ADS_METADATA_ACCESS_TOKEN"
					: null,
				env.META_ADS_METADATA_ACCESS_TOKEN && !env.META_ADS_AD_ACCOUNT_ID
					? "META_ADS_AD_ACCOUNT_ID"
					: null,
			].filter((value): value is string => Boolean(value));

			emitMetaMetadataRuntimeDiagnostic({
				adAccountIds: [group.adAccountId],
				missingConfigKeys:
					missingConfigKeys.length > 0
						? missingConfigKeys
						: ["meta_ads_connections.active"],
				message:
					"Meta metadata resolver has no active token for this account; returning unresolved objects so reporting can display raw IDs.",
			});

			for (const objectId of group.objectIds) {
				const staleCached = cachePolicy.staleResolved.get(
					buildResolutionKey(group.adAccountId, group.objectType, objectId),
				);

				if (staleCached) {
					resolved.push(staleCached);
					staleCacheHitCount += 1;
					continue;
				}

				apiUnresolved.push({
					adAccountId: group.adAccountId,
					objectType: group.objectType,
					objectId,
					reason: "missing_connection",
				});
			}
			continue;
		}

		try {
			apiRequestCount += 1;
			apiLookupObjectCount += group.objectIds.length;
			const lookupResult = await (options.apiLookup ?? fetchMetaObjectsByIds)({
				adAccountId: group.adAccountId,
				accessToken: connection?.access_token ?? "",
				objectType: group.objectType,
				objectIds: group.objectIds,
			});

			for (const objectId of group.objectIds) {
				const apiObject = lookupResult.get(objectId);
				const objectName = collapseWhitespace(apiObject?.name);

				if (!objectName || apiObject?.objectType !== group.objectType) {
					apiUnresolved.push({
						adAccountId: group.adAccountId,
						objectType: group.objectType,
						objectId,
						reason: "meta_api_not_found",
					});
					continue;
				}

				apiResolved.push({
					adAccountId: group.adAccountId,
					objectType: group.objectType,
					objectId,
					objectName,
					status: collapseWhitespace(apiObject.status),
					source: "meta_api",
					lastFetchedAt: fetchedAt.toISOString(),
				});
			}
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			apiFailureCount += group.objectIds.length;

			for (const objectId of group.objectIds) {
				const staleCached = cachePolicy.staleResolved.get(
					buildResolutionKey(group.adAccountId, group.objectType, objectId),
				);

				if (staleCached) {
					resolved.push(staleCached);
					staleCacheHitCount += 1;
					continue;
				}

				apiUnresolved.push({
					adAccountId: group.adAccountId,
					objectType: group.objectType,
					objectId,
					reason: "meta_api_error",
					error: message,
				});
			}
		}
	}

	await upsertSuccessfulMetaMetadataCache(apiResolved, fetchedAt);
	await markFailedMetaMetadataLookups(apiUnresolved, fetchedAt);

	const combinedUnresolved = [...unresolved, ...apiUnresolved];
	const unresolvedReasons = summarizeUnresolvedReasons(combinedUnresolved);

	emitMetaMetadataLookupSummaryLog({
		resolutionScope: "campaign_adset_metadata",
		requestedCount,
		normalizedRequestCount,
		invalidIdCount,
		cacheHitCount: resolved.length,
		staleCacheHitCount,
		recentFailureCacheHitCount,
		cacheMissCount,
		apiRequestCount,
		apiLookupObjectCount,
		apiResolvedCount: apiResolved.length,
		apiNotFoundCount: apiUnresolved.filter(
			(entry) => entry.reason === "meta_api_not_found",
		).length,
		apiFailureCount,
		missingConnectionCount: apiUnresolved.filter(
			(entry) => entry.reason === "missing_connection",
		).length,
		unresolvedCount: combinedUnresolved.length,
		unresolvedEntityIds: combinedUnresolved.map((entry) => entry.objectId),
		unresolvedReasons,
	});

	return {
		resolved: [...resolved, ...apiResolved],
		unresolved: combinedUnresolved,
	};
}

function summarizeUnresolvedReasons(
	unresolved: MetaMetadataUnresolvedObject[],
): Record<string, number> {
	return unresolved.reduce<Record<string, number>>((summary, entry) => {
		summary[entry.reason] = (summary[entry.reason] ?? 0) + 1;
		return summary;
	}, {});
}
