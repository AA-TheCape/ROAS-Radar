import { env } from "../../config/env.js";
import { query } from "../../db/pool.js";

const META_GRAPH_BASE_URL = "https://graph.facebook.com";
const META_RESOLVABLE_OBJECT_TYPES = ["campaign", "adset"] as const;
const META_OBJECT_FIELDS = "id,name,effective_status,status";

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
	objectIds: string[];
}) => Promise<Map<string, MetaMetadataApiObject>>;

type MetaMetadataApiObject = {
	id: string;
	name: string | null;
	status: string | null;
};

type MetaMetadataCacheRow = {
	ad_account_id: string;
	object_type: MetaMetadataObjectType;
	object_id: string;
	object_name: string | null;
	status: string | null;
	last_fetched_at: Date | null;
};

type MetaMetadataConnectionRow = {
	ad_account_id: string;
	access_token: string;
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

type MetaGraphBatchResponse = Record<string, MetaGraphObjectResponse>;

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

async function loadCachedMetaMetadata(
	requests: Array<{
		adAccountId: string;
		objectType: MetaMetadataObjectType;
		objectIds: string[];
	}>,
): Promise<Map<string, MetaMetadataResolvedObject>> {
	if (requests.length === 0) {
		return new Map();
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
        c.last_fetched_at
      FROM meta_ads_metadata_cache c
      JOIN jsonb_to_recordset($1::jsonb) AS requested(
        ad_account_id text,
        object_type text,
        object_id text
      )
        ON requested.ad_account_id = c.ad_account_id
       AND requested.object_type = c.object_type
       AND requested.object_id = c.object_id
      WHERE c.last_fetched_at IS NOT NULL
        AND c.object_name IS NOT NULL
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

	const resolved = new Map<string, MetaMetadataResolvedObject>();

	for (const row of result.rows) {
		const objectName = collapseWhitespace(row.object_name);

		if (!objectName) {
			continue;
		}

		resolved.set(
			buildResolutionKey(row.ad_account_id, row.object_type, row.object_id),
			{
				adAccountId: row.ad_account_id,
				objectType: row.object_type,
				objectId: row.object_id,
				objectName,
				status: collapseWhitespace(row.status),
				source: "cache",
				lastFetchedAt: row.last_fetched_at?.toISOString() ?? null,
			},
		);
	}

	return resolved;
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

	if (normalizedAccountIds.length === 0 || !env.META_ADS_ENCRYPTION_KEY) {
		return new Map();
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

	const connections = new Map<string, MetaMetadataConnectionRow>();

	for (const row of result.rows) {
		if (!connections.has(row.ad_account_id)) {
			connections.set(row.ad_account_id, row);
		}
	}

	return connections;
}

async function fetchMetaObjectsByIds(input: {
	accessToken: string;
	objectIds: string[];
}): Promise<Map<string, MetaMetadataApiObject>> {
	const resolved = new Map<string, MetaMetadataApiObject>();

	for (let index = 0; index < input.objectIds.length; index += 50) {
		const chunk = input.objectIds.slice(index, index + 50);
		const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/`);
		url.searchParams.set("access_token", input.accessToken);
		url.searchParams.set("ids", chunk.join(","));
		url.searchParams.set("fields", META_OBJECT_FIELDS);

		const response = await fetch(url);
		const payload = (await response.json()) as
			| MetaGraphBatchResponse
			| { error?: { message?: string } };

		if (!response.ok) {
			const message = (payload as { error?: { message?: string } }).error
				?.message;
			throw new Error(
				message ??
					`Meta Ads metadata lookup failed with status ${response.status}`,
			);
		}

		for (const objectId of chunk) {
			const entry = (payload as MetaGraphBatchResponse)[objectId];
			const id = normalizeString(entry?.id);
			const name = collapseWhitespace(entry?.name);

			if (!id || !name || entry?.error) {
				continue;
			}

			resolved.set(objectId, {
				id,
				name,
				status: collapseWhitespace(entry.effective_status ?? entry.status),
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
	} = {},
): Promise<MetaMetadataResolutionResult> {
	const normalizedRequests: Array<{
		adAccountId: string;
		objectType: MetaMetadataObjectType;
		objectIds: string[];
	}> = [];
	const unresolved: MetaMetadataUnresolvedObject[] = [];

	for (const request of requests) {
		const adAccountId = normalizeAdAccountId(request.adAccountId);

		if (!adAccountId) {
			continue;
		}

		const { validIds, invalidIds } = normalizeObjectIds(request.objectIds);

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

	const cached = await loadCachedMetaMetadata(normalizedRequests);
	const resolved = [...cached.values()];
	const missingByAccountType = new Map<
		string,
		{
			adAccountId: string;
			objectType: MetaMetadataObjectType;
			objectIds: string[];
		}
	>();

	for (const request of normalizedRequests) {
		for (const objectId of request.objectIds) {
			if (
				cached.has(
					buildResolutionKey(request.adAccountId, request.objectType, objectId),
				)
			) {
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

	if (missingByAccountType.size === 0) {
		return { resolved, unresolved };
	}

	const connections = options.apiLookup
		? new Map<string, MetaMetadataConnectionRow>()
		: await loadActiveMetaMetadataConnections([
				...new Set(
					[...missingByAccountType.values()].map((group) => group.adAccountId),
				),
			]);
	const fetchedAt = options.now ?? new Date();
	const apiResolved: MetaMetadataResolvedObject[] = [];
	const apiUnresolved: MetaMetadataUnresolvedObject[] = [];

	for (const group of missingByAccountType.values()) {
		const connection = connections.get(group.adAccountId);

		if (!options.apiLookup && !connection) {
			for (const objectId of group.objectIds) {
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
			const lookupResult = await (options.apiLookup ?? fetchMetaObjectsByIds)({
				adAccountId: group.adAccountId,
				accessToken: connection?.access_token ?? "",
				objectIds: group.objectIds,
			});

			for (const objectId of group.objectIds) {
				const apiObject = lookupResult.get(objectId);

				if (!apiObject?.name) {
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
					objectName: apiObject.name,
					status: apiObject.status,
					source: "meta_api",
					lastFetchedAt: fetchedAt.toISOString(),
				});
			}
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);

			for (const objectId of group.objectIds) {
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

	return {
		resolved: [...resolved, ...apiResolved],
		unresolved: [...unresolved, ...apiUnresolved],
	};
}
