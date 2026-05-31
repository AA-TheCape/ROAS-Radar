import { z } from "zod";

export const RECOVERY_JOB_CONTRACT_VERSION = 1 as const;
export const RECOVERY_JOB_JSON_SCHEMA_DRAFT =
	"https://json-schema.org/draft/2020-12/schema" as const;
export const RECOVERY_SOURCE_PRECEDENCE = [
	"shopify",
	"ga4",
	"ad_platforms",
] as const;
export const RECOVERY_JOB_TYPES = [
	"shopify_attribution_hint_recovery",
	"ga4_fallback_unattributed_recovery",
	"campaign_metadata_api_refresh",
	"campaign_metadata_history_backfill",
	"shopify_order_reimport",
	"order_attribution_backfill",
] as const;
export const RECOVERY_JOB_MODES = ["manual", "scheduled", "automatic"] as const;
export const RECOVERY_JOB_STATUSES = [
	"queued",
	"running",
	"succeeded",
	"partial_failure",
	"failed",
	"cancelled",
] as const;
export const RECOVERY_AD_PLATFORMS = ["google_ads", "meta_ads"] as const;
export const RECOVERY_METADATA_ENTITY_TYPES = ["campaign", "adset", "ad"] as const;
export const RECOVERY_CLICK_ID_TYPES = [
	"gclid",
	"gbraid",
	"wbraid",
	"fbclid",
	"ttclid",
	"msclkid",
] as const;

const ISO_TIMESTAMP_REGEX =
	/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;
const DATE_ONLY_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const SHA_256_REGEX = /^[0-9a-f]{64}$/i;

type JsonSchemaDocument = Record<string, unknown>;

const normalizeString = (value: string | null | undefined): string | null => {
	const trimmed = value?.trim();
	return trimmed ? trimmed : null;
};

const isoTimestampSchema = z
	.string()
	.trim()
	.refine((value) => ISO_TIMESTAMP_REGEX.test(value), "Invalid ISO-8601 timestamp")
	.transform((value) => new Date(value).toISOString());

const isoTimestampOrNullSchema = z
	.union([z.string(), z.null(), z.undefined()])
	.transform((value) => normalizeString(value))
	.refine((value) => value === null || ISO_TIMESTAMP_REGEX.test(value), "Invalid ISO-8601 timestamp")
	.transform((value) => (value ? new Date(value).toISOString() : null));

const dateOnlyOrNullSchema = z
	.union([z.string(), z.null(), z.undefined()])
	.transform((value) => normalizeString(value))
	.refine((value) => value === null || DATE_ONLY_REGEX.test(value), "Use YYYY-MM-DD.")
	.transform((value) => value ?? null);

const nullableTextSchema = z
	.union([z.string(), z.null(), z.undefined()])
	.transform((value) => normalizeString(value));

const requiredTextSchema = z
	.string()
	.trim()
	.min(1)
	.transform((value) => value);

const recoveryJsonSchema: z.ZodType<unknown> = z.lazy(() =>
	z.union([
		z.string(),
		z.number(),
		z.boolean(),
		z.null(),
		z.array(recoveryJsonSchema),
		z.record(z.string(), recoveryJsonSchema),
	]),
);

const recoveryJsonObjectSchema = z.record(z.string(), recoveryJsonSchema);

export const recoveryJobRequestSchema = z
	.object({
		schemaVersion: z.literal(RECOVERY_JOB_CONTRACT_VERSION),
		jobType: z.enum(RECOVERY_JOB_TYPES),
		mode: z.enum(RECOVERY_JOB_MODES).default("manual"),
		initiatedBy: requiredTextSchema,
		dryRun: z.boolean().default(true),
		timeRangeStart: isoTimestampSchema,
		timeRangeEnd: isoTimestampSchema,
		scopeKey: nullableTextSchema.transform((value) => value ?? "global"),
		idempotencyKey: nullableTextSchema,
		resumeFromRunId: nullableTextSchema,
		rerunOfRunId: nullableTextSchema,
		inputParameters: recoveryJsonObjectSchema.default({}),
	})
	.refine((value) => value.timeRangeStart <= value.timeRangeEnd, {
		message: "timeRangeStart must be on or before timeRangeEnd.",
		path: ["timeRangeEnd"],
	});

export const recoveryJobFailureSchema = z.object({
	recordType: nullableTextSchema,
	recordKey: nullableTextSchema,
	code: requiredTextSchema,
	message: requiredTextSchema,
	sourceSystem: z.enum(RECOVERY_SOURCE_PRECEDENCE).nullable(),
	retryable: z.boolean(),
});

export const recoveryJobReportSchema = z.object({
	schemaVersion: z.literal(RECOVERY_JOB_CONTRACT_VERSION),
	jobId: requiredTextSchema,
	jobType: z.enum(RECOVERY_JOB_TYPES),
	status: z.enum(RECOVERY_JOB_STATUSES),
	startedAt: isoTimestampOrNullSchema,
	completedAt: isoTimestampOrNullSchema,
	dryRun: z.boolean(),
	sourcePrecedence: z.tuple([
		z.literal("shopify"),
		z.literal("ga4"),
		z.literal("ad_platforms"),
	]),
	counters: z.object({
		recordsDiscovered: z.number().int().nonnegative(),
		recordsProcessed: z.number().int().nonnegative(),
		recordsSucceeded: z.number().int().nonnegative(),
		recordsFailed: z.number().int().nonnegative(),
		recordsSkipped: z.number().int().nonnegative(),
		sideEffectsAttempted: z.number().int().nonnegative(),
		sideEffectsSucceeded: z.number().int().nonnegative(),
		sideEffectsSuppressed: z.number().int().nonnegative(),
	}),
	artifacts: z
		.array(
			z.object({
				name: requiredTextSchema,
				uri: requiredTextSchema,
				contentType: nullableTextSchema,
				sha256: nullableTextSchema.refine(
					(value) => value === null || SHA_256_REGEX.test(value),
					"Invalid SHA-256 digest",
				),
			}),
		)
		.default([]),
	failures: z.array(recoveryJobFailureSchema).default([]),
});

export const shopifyRawPayloadSnapshotSchema = z.object({
	schemaVersion: z.literal(RECOVERY_JOB_CONTRACT_VERSION),
	snapshotId: requiredTextSchema,
	shopDomain: requiredTextSchema,
	shopifyOrderId: requiredTextSchema,
	capturedAt: isoTimestampSchema,
	payloadVersion: z.number().int().positive(),
	payloadSha256: z.string().regex(SHA_256_REGEX, "Invalid SHA-256 digest"),
	storageUri: nullableTextSchema,
	rawPayload: recoveryJsonObjectSchema,
	normalized: z.object({
		orderName: nullableTextSchema,
		processedAt: isoTimestampOrNullSchema,
		createdAtShopify: isoTimestampOrNullSchema,
		currencyCode: nullableTextSchema.transform((value) => value?.toUpperCase() ?? null),
		totalPrice: nullableTextSchema,
		subtotalPrice: nullableTextSchema,
		landingSite: nullableTextSchema,
		referringSite: nullableTextSchema,
		checkoutToken: nullableTextSchema,
		cartToken: nullableTextSchema,
		customerId: nullableTextSchema,
		sourceName: nullableTextSchema,
	}),
});

export const shopifyAttributionHintSchema = z.object({
	schemaVersion: z.literal(RECOVERY_JOB_CONTRACT_VERSION),
	shopifyOrderId: requiredTextSchema,
	extractedAt: isoTimestampSchema,
	hintSource: z.enum(["note_attributes", "landing_site", "attributes_array", "client_details"]),
	source: nullableTextSchema.transform((value) => value?.toLowerCase() ?? null),
	medium: nullableTextSchema.transform((value) => value?.toLowerCase() ?? null),
	campaign: nullableTextSchema.transform((value) => value?.toLowerCase() ?? null),
	content: nullableTextSchema,
	term: nullableTextSchema,
	clickIdType: z
		.union([z.enum(RECOVERY_CLICK_ID_TYPES), z.null(), z.undefined()])
		.transform((value) => value ?? null),
	clickIdValue: nullableTextSchema,
	landingSite: nullableTextSchema,
	referringSite: nullableTextSchema,
	confidenceScore: z.number().min(0).max(1),
	confidenceLabel: z.enum(["low", "medium", "high"]),
	rawKeys: z.array(requiredTextSchema).default([]),
	sourcePrecedenceRank: z.literal(1),
});

export const ga4EnrichmentFieldsSchema = z.object({
	schemaVersion: z.literal(RECOVERY_JOB_CONTRACT_VERSION),
	shopifyOrderId: requiredTextSchema,
	ga4PropertyId: requiredTextSchema,
	ga4EventDate: z.string().regex(DATE_ONLY_REGEX, "Use YYYY-MM-DD."),
	enrichedAt: isoTimestampSchema,
	clientId: nullableTextSchema,
	sessionId: nullableTextSchema,
	userPseudoId: nullableTextSchema,
	transactionId: nullableTextSchema,
	source: nullableTextSchema.transform((value) => value?.toLowerCase() ?? null),
	medium: nullableTextSchema.transform((value) => value?.toLowerCase() ?? null),
	campaign: nullableTextSchema.transform((value) => value?.toLowerCase() ?? null),
	content: nullableTextSchema,
	term: nullableTextSchema,
	clickIdType: z
		.union([z.enum(RECOVERY_CLICK_ID_TYPES), z.null(), z.undefined()])
		.transform((value) => value ?? null),
	clickIdValue: nullableTextSchema,
	collectedTrafficSource: z
		.union([recoveryJsonObjectSchema, z.null(), z.undefined()])
		.transform((value) => value ?? null),
	sourcePrecedenceRank: z.literal(2),
});

export const campaignMetadataRefreshPayloadSchema = z
	.object({
		schemaVersion: z.literal(RECOVERY_JOB_CONTRACT_VERSION),
		runId: nullableTextSchema,
		requestedBy: requiredTextSchema,
		workerId: requiredTextSchema,
		mode: z.enum(["api_refresh", "history_backfill"]),
		platforms: z.array(z.enum(RECOVERY_AD_PLATFORMS)).default([]),
		startDate: dateOnlyOrNullSchema,
		endDate: dateOnlyOrNullSchema,
		campaignIds: z.array(requiredTextSchema).default([]),
		dryRun: z.boolean().default(true),
		maxAttempts: z.number().int().positive().optional(),
		entities: z
			.array(
				z.object({
					platform: z.enum(RECOVERY_AD_PLATFORMS),
					accountId: requiredTextSchema,
					entityType: z.enum(RECOVERY_METADATA_ENTITY_TYPES),
					entityId: requiredTextSchema,
					latestName: nullableTextSchema,
					lastSeenAt: isoTimestampOrNullSchema,
				}),
			)
			.default([]),
		sourcePrecedenceRank: z.literal(3),
	})
	.refine(
		(value) =>
			value.startDate === null ||
			value.endDate === null ||
			value.startDate <= value.endDate,
		{
			message: "startDate must be on or before endDate.",
			path: ["endDate"],
		},
	);

const jsonNullableString: JsonSchemaDocument = { type: ["string", "null"] };
const jsonTimestamp: JsonSchemaDocument = {
	type: "string",
	format: "date-time",
	pattern: ISO_TIMESTAMP_REGEX.source,
};
const jsonTimestampOrNull: JsonSchemaDocument = {
	anyOf: [jsonTimestamp, { type: "null" }],
};
const jsonDateOrNull: JsonSchemaDocument = {
	type: ["string", "null"],
	pattern: DATE_ONLY_REGEX.source,
};
const jsonPayloadObject: JsonSchemaDocument = {
	type: "object",
	additionalProperties: true,
};

export const recoveryJobRequestJsonSchema: JsonSchemaDocument = {
	$schema: RECOVERY_JOB_JSON_SCHEMA_DRAFT,
	title: "RecoveryJobRequestV1",
	type: "object",
	additionalProperties: false,
	required: [
		"schemaVersion",
		"jobType",
		"mode",
		"initiatedBy",
		"dryRun",
		"timeRangeStart",
		"timeRangeEnd",
		"scopeKey",
		"idempotencyKey",
		"resumeFromRunId",
		"rerunOfRunId",
		"inputParameters",
	],
	properties: {
		schemaVersion: { const: RECOVERY_JOB_CONTRACT_VERSION, type: "integer" },
		jobType: { type: "string", enum: [...RECOVERY_JOB_TYPES] },
		mode: { type: "string", enum: [...RECOVERY_JOB_MODES] },
		initiatedBy: { type: "string", minLength: 1 },
		dryRun: { type: "boolean" },
		timeRangeStart: jsonTimestamp,
		timeRangeEnd: jsonTimestamp,
		scopeKey: { type: "string", minLength: 1 },
		idempotencyKey: jsonNullableString,
		resumeFromRunId: jsonNullableString,
		rerunOfRunId: jsonNullableString,
		inputParameters: jsonPayloadObject,
	},
};

export const recoveryJobReportJsonSchema: JsonSchemaDocument = {
	$schema: RECOVERY_JOB_JSON_SCHEMA_DRAFT,
	title: "RecoveryJobReportV1",
	type: "object",
	additionalProperties: false,
	required: [
		"schemaVersion",
		"jobId",
		"jobType",
		"status",
		"startedAt",
		"completedAt",
		"dryRun",
		"sourcePrecedence",
		"counters",
		"artifacts",
		"failures",
	],
	properties: {
		schemaVersion: { const: RECOVERY_JOB_CONTRACT_VERSION, type: "integer" },
		jobId: { type: "string", minLength: 1 },
		jobType: { type: "string", enum: [...RECOVERY_JOB_TYPES] },
		status: { type: "string", enum: [...RECOVERY_JOB_STATUSES] },
		startedAt: jsonTimestampOrNull,
		completedAt: jsonTimestampOrNull,
		dryRun: { type: "boolean" },
		sourcePrecedence: {
			type: "array",
			prefixItems: [
				{ const: "shopify" },
				{ const: "ga4" },
				{ const: "ad_platforms" },
			],
			minItems: 3,
			maxItems: 3,
		},
		counters: {
			type: "object",
			additionalProperties: false,
			required: [
				"recordsDiscovered",
				"recordsProcessed",
				"recordsSucceeded",
				"recordsFailed",
				"recordsSkipped",
				"sideEffectsAttempted",
				"sideEffectsSucceeded",
				"sideEffectsSuppressed",
			],
			properties: {
				recordsDiscovered: { type: "integer", minimum: 0 },
				recordsProcessed: { type: "integer", minimum: 0 },
				recordsSucceeded: { type: "integer", minimum: 0 },
				recordsFailed: { type: "integer", minimum: 0 },
				recordsSkipped: { type: "integer", minimum: 0 },
				sideEffectsAttempted: { type: "integer", minimum: 0 },
				sideEffectsSucceeded: { type: "integer", minimum: 0 },
				sideEffectsSuppressed: { type: "integer", minimum: 0 },
			},
		},
		artifacts: {
			type: "array",
			items: {
				type: "object",
				additionalProperties: false,
				required: ["name", "uri", "contentType", "sha256"],
				properties: {
					name: { type: "string", minLength: 1 },
					uri: { type: "string", minLength: 1 },
					contentType: jsonNullableString,
					sha256: { type: ["string", "null"], pattern: SHA_256_REGEX.source },
				},
			},
		},
		failures: {
			type: "array",
			items: {
				type: "object",
				additionalProperties: false,
				required: ["recordType", "recordKey", "code", "message", "sourceSystem", "retryable"],
				properties: {
					recordType: jsonNullableString,
					recordKey: jsonNullableString,
					code: { type: "string", minLength: 1 },
					message: { type: "string", minLength: 1 },
					sourceSystem: { type: ["string", "null"], enum: [...RECOVERY_SOURCE_PRECEDENCE, null] },
					retryable: { type: "boolean" },
				},
			},
		},
	},
};

export const shopifyRawPayloadSnapshotJsonSchema: JsonSchemaDocument = {
	$schema: RECOVERY_JOB_JSON_SCHEMA_DRAFT,
	title: "ShopifyRawPayloadSnapshotV1",
	type: "object",
	additionalProperties: false,
	required: [
		"schemaVersion",
		"snapshotId",
		"shopDomain",
		"shopifyOrderId",
		"capturedAt",
		"payloadVersion",
		"payloadSha256",
		"storageUri",
		"rawPayload",
		"normalized",
	],
	properties: {
		schemaVersion: { const: RECOVERY_JOB_CONTRACT_VERSION, type: "integer" },
		snapshotId: { type: "string", minLength: 1 },
		shopDomain: { type: "string", minLength: 1 },
		shopifyOrderId: { type: "string", minLength: 1 },
		capturedAt: jsonTimestamp,
		payloadVersion: { type: "integer", minimum: 1 },
		payloadSha256: { type: "string", pattern: SHA_256_REGEX.source },
		storageUri: jsonNullableString,
		rawPayload: jsonPayloadObject,
		normalized: {
			type: "object",
			additionalProperties: false,
			required: [
				"orderName",
				"processedAt",
				"createdAtShopify",
				"currencyCode",
				"totalPrice",
				"subtotalPrice",
				"landingSite",
				"referringSite",
				"checkoutToken",
				"cartToken",
				"customerId",
				"sourceName",
			],
			properties: {
				orderName: jsonNullableString,
				processedAt: jsonTimestampOrNull,
				createdAtShopify: jsonTimestampOrNull,
				currencyCode: jsonNullableString,
				totalPrice: jsonNullableString,
				subtotalPrice: jsonNullableString,
				landingSite: jsonNullableString,
				referringSite: jsonNullableString,
				checkoutToken: jsonNullableString,
				cartToken: jsonNullableString,
				customerId: jsonNullableString,
				sourceName: jsonNullableString,
			},
		},
	},
};

export const shopifyAttributionHintJsonSchema: JsonSchemaDocument = {
	$schema: RECOVERY_JOB_JSON_SCHEMA_DRAFT,
	title: "ShopifyAttributionHintV1",
	type: "object",
	additionalProperties: false,
	required: [
		"schemaVersion",
		"shopifyOrderId",
		"extractedAt",
		"hintSource",
		"source",
		"medium",
		"campaign",
		"content",
		"term",
		"clickIdType",
		"clickIdValue",
		"landingSite",
		"referringSite",
		"confidenceScore",
		"confidenceLabel",
		"rawKeys",
		"sourcePrecedenceRank",
	],
	properties: {
		schemaVersion: { const: RECOVERY_JOB_CONTRACT_VERSION, type: "integer" },
		shopifyOrderId: { type: "string", minLength: 1 },
		extractedAt: jsonTimestamp,
		hintSource: {
			type: "string",
			enum: ["note_attributes", "landing_site", "attributes_array", "client_details"],
		},
		source: jsonNullableString,
		medium: jsonNullableString,
		campaign: jsonNullableString,
		content: jsonNullableString,
		term: jsonNullableString,
		clickIdType: { type: ["string", "null"], enum: [...RECOVERY_CLICK_ID_TYPES, null] },
		clickIdValue: jsonNullableString,
		landingSite: jsonNullableString,
		referringSite: jsonNullableString,
		confidenceScore: { type: "number", minimum: 0, maximum: 1 },
		confidenceLabel: { type: "string", enum: ["low", "medium", "high"] },
		rawKeys: { type: "array", items: { type: "string", minLength: 1 } },
		sourcePrecedenceRank: { const: 1, type: "integer" },
	},
};

export const ga4EnrichmentFieldsJsonSchema: JsonSchemaDocument = {
	$schema: RECOVERY_JOB_JSON_SCHEMA_DRAFT,
	title: "Ga4EnrichmentFieldsV1",
	type: "object",
	additionalProperties: false,
	required: [
		"schemaVersion",
		"shopifyOrderId",
		"ga4PropertyId",
		"ga4EventDate",
		"enrichedAt",
		"clientId",
		"sessionId",
		"userPseudoId",
		"transactionId",
		"source",
		"medium",
		"campaign",
		"content",
		"term",
		"clickIdType",
		"clickIdValue",
		"collectedTrafficSource",
		"sourcePrecedenceRank",
	],
	properties: {
		schemaVersion: { const: RECOVERY_JOB_CONTRACT_VERSION, type: "integer" },
		shopifyOrderId: { type: "string", minLength: 1 },
		ga4PropertyId: { type: "string", minLength: 1 },
		ga4EventDate: { type: "string", pattern: DATE_ONLY_REGEX.source },
		enrichedAt: jsonTimestamp,
		clientId: jsonNullableString,
		sessionId: jsonNullableString,
		userPseudoId: jsonNullableString,
		transactionId: jsonNullableString,
		source: jsonNullableString,
		medium: jsonNullableString,
		campaign: jsonNullableString,
		content: jsonNullableString,
		term: jsonNullableString,
		clickIdType: { type: ["string", "null"], enum: [...RECOVERY_CLICK_ID_TYPES, null] },
		clickIdValue: jsonNullableString,
		collectedTrafficSource: { anyOf: [jsonPayloadObject, { type: "null" }] },
		sourcePrecedenceRank: { const: 2, type: "integer" },
	},
};

export const campaignMetadataRefreshPayloadJsonSchema: JsonSchemaDocument = {
	$schema: RECOVERY_JOB_JSON_SCHEMA_DRAFT,
	title: "CampaignMetadataRefreshPayloadV1",
	type: "object",
	additionalProperties: false,
	required: [
		"schemaVersion",
		"runId",
		"requestedBy",
		"workerId",
		"mode",
		"platforms",
		"startDate",
		"endDate",
		"campaignIds",
		"dryRun",
		"entities",
		"sourcePrecedenceRank",
	],
	properties: {
		schemaVersion: { const: RECOVERY_JOB_CONTRACT_VERSION, type: "integer" },
		runId: jsonNullableString,
		requestedBy: { type: "string", minLength: 1 },
		workerId: { type: "string", minLength: 1 },
		mode: { type: "string", enum: ["api_refresh", "history_backfill"] },
		platforms: { type: "array", items: { type: "string", enum: [...RECOVERY_AD_PLATFORMS] } },
		startDate: jsonDateOrNull,
		endDate: jsonDateOrNull,
		campaignIds: { type: "array", items: { type: "string", minLength: 1 } },
		dryRun: { type: "boolean" },
		maxAttempts: { type: "integer", minimum: 1 },
		entities: {
			type: "array",
			items: {
				type: "object",
				additionalProperties: false,
				required: ["platform", "accountId", "entityType", "entityId", "latestName", "lastSeenAt"],
				properties: {
					platform: { type: "string", enum: [...RECOVERY_AD_PLATFORMS] },
					accountId: { type: "string", minLength: 1 },
					entityType: { type: "string", enum: [...RECOVERY_METADATA_ENTITY_TYPES] },
					entityId: { type: "string", minLength: 1 },
					latestName: jsonNullableString,
					lastSeenAt: jsonTimestampOrNull,
				},
			},
		},
		sourcePrecedenceRank: { const: 3, type: "integer" },
	},
};

export const recoveryJobFrameworkJsonSchemas = {
	RecoveryJobRequestV1: recoveryJobRequestJsonSchema,
	RecoveryJobReportV1: recoveryJobReportJsonSchema,
	ShopifyRawPayloadSnapshotV1: shopifyRawPayloadSnapshotJsonSchema,
	ShopifyAttributionHintV1: shopifyAttributionHintJsonSchema,
	Ga4EnrichmentFieldsV1: ga4EnrichmentFieldsJsonSchema,
	CampaignMetadataRefreshPayloadV1: campaignMetadataRefreshPayloadJsonSchema,
} as const;

export type RecoverySourcePrecedence = typeof RECOVERY_SOURCE_PRECEDENCE;
export type RecoveryJobType = (typeof RECOVERY_JOB_TYPES)[number];
export type RecoveryJobMode = (typeof RECOVERY_JOB_MODES)[number];
export type RecoveryJobStatus = (typeof RECOVERY_JOB_STATUSES)[number];
export type RecoveryAdPlatform = (typeof RECOVERY_AD_PLATFORMS)[number];
export type RecoveryMetadataEntityType =
	(typeof RECOVERY_METADATA_ENTITY_TYPES)[number];
export type RecoveryJobRequest = z.infer<typeof recoveryJobRequestSchema>;
export type RecoveryJobReport = z.infer<typeof recoveryJobReportSchema>;
export type ShopifyRawPayloadSnapshot = z.infer<
	typeof shopifyRawPayloadSnapshotSchema
>;
export type ShopifyAttributionHint = z.infer<typeof shopifyAttributionHintSchema>;
export type Ga4EnrichmentFields = z.infer<typeof ga4EnrichmentFieldsSchema>;
export type CampaignMetadataRefreshPayload = z.infer<
	typeof campaignMetadataRefreshPayloadSchema
>;

export function normalizeRecoveryJobRequest(input: unknown): RecoveryJobRequest {
	return recoveryJobRequestSchema.parse(input);
}

export function normalizeRecoveryJobReport(input: unknown): RecoveryJobReport {
	return recoveryJobReportSchema.parse(input);
}

export function normalizeShopifyRawPayloadSnapshot(
	input: unknown,
): ShopifyRawPayloadSnapshot {
	return shopifyRawPayloadSnapshotSchema.parse(input);
}

export function normalizeShopifyAttributionHint(input: unknown): ShopifyAttributionHint {
	return shopifyAttributionHintSchema.parse(input);
}

export function normalizeGa4EnrichmentFields(input: unknown): Ga4EnrichmentFields {
	return ga4EnrichmentFieldsSchema.parse(input);
}

export function normalizeCampaignMetadataRefreshPayload(
	input: unknown,
): CampaignMetadataRefreshPayload {
	return campaignMetadataRefreshPayloadSchema.parse(input);
}
