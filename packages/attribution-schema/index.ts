import { z } from 'zod';

export {
  ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
  ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
  normalizeOrderAttributionBackfillRequest,
  orderAttributionBackfillEnqueueResponseSchema,
  orderAttributionBackfillFailureSchema,
  orderAttributionBackfillJobResponseSchema,
  orderAttributionBackfillJobStatusSchema,
  orderAttributionBackfillReportSchema,
  orderAttributionBackfillRequestSchema,
  orderAttributionBackfillSubmittedOptionsSchema,
  type OrderAttributionBackfillEnqueueResponse,
  type OrderAttributionBackfillFailure,
  type OrderAttributionBackfillJobResponse,
  type OrderAttributionBackfillJobStatus,
  type OrderAttributionBackfillReport,
  type OrderAttributionBackfillRequest,
  type OrderAttributionBackfillSubmittedOptions
} from './order-attribution-backfill.js';

export const ATTRIBUTION_SCHEMA_VERSION = 1 as const;
export const MAX_ATTRIBUTION_URL_LENGTH = 2048;
export const MAX_ATTRIBUTION_TEXT_LENGTH = 255;
export const MAX_SESSION_ID_LENGTH = 36;
export const ATTRIBUTION_CONSENT_STATES = ['granted', 'denied', 'unknown'] as const;
export const ATTRIBUTION_URL_FIELDS = ['landing_url', 'referrer_url', 'page_url'] as const;
export const ATTRIBUTION_UTM_FIELDS = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'] as const;
export const ATTRIBUTION_CLICK_ID_FIELDS = ['gclid', 'gbraid', 'wbraid', 'fbclid', 'ttclid', 'msclkid'] as const;

const ISO_TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

type NullableString = string | null | undefined;

export function normalizeAttributionString(value: NullableString): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

export function normalizeAttributionLowercaseString(value: NullableString): string | null {
  const normalized = normalizeAttributionString(value);
  return normalized ? normalized.toLowerCase() : null;
}

export function normalizeAttributionUrl(value: NullableString, baseUrl?: string): string | null {
  const normalized = normalizeAttributionString(value);

  if (!normalized) {
    return null;
  }

  const url = baseUrl ? new URL(normalized, baseUrl) : new URL(normalized);

  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error('invalid_protocol');
  }

  url.hash = '';
  return url.toString();
}

export function normalizeAttributionUtm(value: NullableString): string | null {
  return normalizeAttributionLowercaseString(value);
}

export function normalizeAttributionClickId(value: NullableString): string | null {
  return normalizeAttributionString(value);
}

export function isAttributionSessionId(value: string | null | undefined): value is string {
  const normalized = normalizeAttributionString(value);
  return Boolean(normalized && normalized.length <= MAX_SESSION_ID_LENGTH && UUID_REGEX.test(normalized));
}

const nullableUrlSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeAttributionString(value))
  .refine((value) => value === null || value.length <= MAX_ATTRIBUTION_URL_LENGTH, {
    message: `String must contain at most ${MAX_ATTRIBUTION_URL_LENGTH} character(s)`
  })
  .superRefine((value, ctx) => {
    if (!value) {
      return;
    }

    try {
      normalizeAttributionUrl(value);
    } catch (error) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: error instanceof Error && error.message === 'invalid_protocol' ? 'URL must use http or https' : 'Invalid URL'
      });
    }
  })
  .transform((value) => (value ? normalizeAttributionUrl(value) : null));

const nullableUtmSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeAttributionString(value))
  .refine((value) => value === null || value.length <= MAX_ATTRIBUTION_TEXT_LENGTH, {
    message: `String must contain at most ${MAX_ATTRIBUTION_TEXT_LENGTH} character(s)`
  })
  .transform((value) => normalizeAttributionUtm(value));

const nullableClickIdSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeAttributionString(value))
  .refine((value) => value === null || value.length <= MAX_ATTRIBUTION_TEXT_LENGTH, {
    message: `String must contain at most ${MAX_ATTRIBUTION_TEXT_LENGTH} character(s)`
  })
  .transform((value) => normalizeAttributionClickId(value));

const isoTimestampSchema = z
  .string()
  .trim()
  .refine((value) => ISO_TIMESTAMP_REGEX.test(value), 'Invalid ISO-8601 timestamp')
  .transform((value) => new Date(value).toISOString());

export const attributionConsentStateSchema = z.enum(ATTRIBUTION_CONSENT_STATES).default('unknown');

export const attributionCaptureV1Schema = z.object({
  schema_version: z.literal(ATTRIBUTION_SCHEMA_VERSION),
  roas_radar_session_id: z.string().uuid().max(MAX_SESSION_ID_LENGTH),
  occurred_at: isoTimestampSchema,
  captured_at: isoTimestampSchema,
  landing_url: nullableUrlSchema,
  referrer_url: nullableUrlSchema,
  page_url: nullableUrlSchema,
  utm_source: nullableUtmSchema,
  utm_medium: nullableUtmSchema,
  utm_campaign: nullableUtmSchema,
  utm_content: nullableUtmSchema,
  utm_term: nullableUtmSchema,
  gclid: nullableClickIdSchema,
  gbraid: nullableClickIdSchema,
  wbraid: nullableClickIdSchema,
  fbclid: nullableClickIdSchema,
  ttclid: nullableClickIdSchema,
  msclkid: nullableClickIdSchema
});

export type AttributionSchemaVersion = typeof ATTRIBUTION_SCHEMA_VERSION;
export type AttributionUrlField = (typeof ATTRIBUTION_URL_FIELDS)[number];
export type AttributionUtmField = (typeof ATTRIBUTION_UTM_FIELDS)[number];
export type AttributionClickIdField = (typeof ATTRIBUTION_CLICK_ID_FIELDS)[number];
export type AttributionCaptureV1 = z.infer<typeof attributionCaptureV1Schema>;
export type AttributionConsentState = z.infer<typeof attributionConsentStateSchema>;

export function normalizeAttributionCaptureV1(input: unknown): AttributionCaptureV1 {
  return attributionCaptureV1Schema.parse(input);
}

export function normalizeAttributionConsentState(input: unknown): AttributionConsentState {
  return attributionConsentStateSchema.parse(input);
}

const nonEmptyLowercaseEnum = <T extends readonly [string, ...string[]]>(values: T) => z.enum(values);

const nullableTextSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeAttributionString(value))
  .refine((value) => value === null || value.length <= MAX_ATTRIBUTION_TEXT_LENGTH, {
    message: `String must contain at most ${MAX_ATTRIBUTION_TEXT_LENGTH} character(s)`
  });

const nullableLowercaseTextSchema = nullableTextSchema.transform((value) => normalizeAttributionLowercaseString(value));

const uuidOrNullSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeAttributionString(value))
  .refine((value) => value === null || UUID_REGEX.test(value), {
    message: 'Invalid UUID'
  });

const isoTimestampOrNullSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeAttributionString(value))
  .refine((value) => value === null || ISO_TIMESTAMP_REGEX.test(value), 'Invalid ISO-8601 timestamp')
  .transform((value) => (value ? new Date(value).toISOString() : null));

const decimalStringSchema = z
  .union([z.string(), z.number()])
  .transform((value) => (typeof value === 'number' ? value.toFixed(2) : value.trim()))
  .refine((value) => /^\d+(?:\.\d+)?$/.test(value), 'Invalid decimal string');

export const ATTRIBUTION_EVIDENCE_SOURCES = [
  'landing_session_id',
  'checkout_token',
  'cart_token',
  'customer_identity',
  'shopify_marketing_hint',
  'ga4_fallback'
] as const;

export const ATTRIBUTION_TOUCHPOINT_SOURCE_KINDS = [
  'session_first_touch',
  'session_event',
  'shopify_hint'
] as const;

export const ATTRIBUTION_INGESTION_SOURCES = [
  'browser',
  'server',
  'request_query',
  'shopify_marketing_hint'
] as const;

export const ATTRIBUTION_ENGAGEMENT_TYPES = ['click', 'view', 'unknown'] as const;
export const ATTRIBUTION_ORDER_TIMESTAMP_SOURCES = [
  'processed_at',
  'created_at_shopify',
  'ingested_at'
] as const;
export const ATTRIBUTION_MODEL_KEYS = [
  'first_touch',
  'last_touch',
  'last_non_direct',
  'linear',
  'clicks_only',
  'hinted_fallback_only'
] as const;
export const ATTRIBUTION_ALLOCATION_STATUSES = [
  'attributed',
  'no_eligible_touches',
  'blocked_by_deterministic',
  'unattributed'
] as const;
export const ATTRIBUTION_LOOKBACK_RULES = ['28d_click', '7d_view', 'mixed'] as const;
export const ATTRIBUTION_EXPLAIN_STAGES = ['candidate_extraction', 'eligibility_filter', 'model_scoring', 'fallback'] as const;
export const ATTRIBUTION_EXPLAIN_DECISIONS = ['included', 'excluded', 'winner', 'fallback_used', 'no_credit'] as const;

export const attributionHintConfidenceLabelSchema = nonEmptyLowercaseEnum(['low', 'medium', 'high']);

export const attributionHintInputV1Schema = z.object({
  hint_source_system: z.literal('shopify_order'),
  hint_type: nonEmptyLowercaseEnum(['note_attributes', 'landing_site', 'attributes_array']),
  source: nullableLowercaseTextSchema,
  medium: nullableLowercaseTextSchema,
  campaign: nullableLowercaseTextSchema,
  content: nullableLowercaseTextSchema,
  term: nullableLowercaseTextSchema,
  click_id_type: z.enum(ATTRIBUTION_CLICK_ID_FIELDS).nullable(),
  click_id_value: nullableTextSchema,
  hint_confidence_score: decimalStringSchema,
  hint_confidence_label: attributionHintConfidenceLabelSchema,
  raw_hint_keys: z.array(z.string().min(1)).default([])
});

export const attributionOrderInputV1Schema = z.object({
  schema_version: z.literal(1),
  order_id: z.string().min(1),
  order_platform: z.literal('shopify'),
  order_occurred_at_utc: isoTimestampSchema,
  order_timestamp_source: z.enum(ATTRIBUTION_ORDER_TIMESTAMP_SOURCES),
  currency_code: z.string().trim().min(3).max(16).transform((value) => value.toUpperCase()),
  subtotal_amount: decimalStringSchema,
  total_amount: decimalStringSchema,
  landing_session_id: uuidOrNullSchema,
  checkout_token: nullableTextSchema,
  cart_token: nullableTextSchema,
  shopify_customer_id: nullableTextSchema,
  email_hash: z
    .union([z.string(), z.null(), z.undefined()])
    .transform((value) => normalizeAttributionString(value))
    .refine((value) => value === null || /^[0-9a-f]{64}$/i.test(value), 'Invalid email hash'),
  source_name: nullableTextSchema,
  identity_journey_id: uuidOrNullSchema,
  raw_order_ref: z.record(z.string(), z.unknown()).nullable()
});

export const attributionTouchpointInputV1Schema = z.object({
  schema_version: z.literal(1),
  touchpoint_id: z.string().min(1).max(MAX_ATTRIBUTION_TEXT_LENGTH),
  session_id: uuidOrNullSchema,
  identity_journey_id: uuidOrNullSchema,
  touchpoint_occurred_at_utc: isoTimestampSchema,
  touchpoint_captured_at_utc: isoTimestampSchema,
  touchpoint_source_kind: z.enum(ATTRIBUTION_TOUCHPOINT_SOURCE_KINDS),
  ingestion_source: z.enum(ATTRIBUTION_INGESTION_SOURCES),
  source: nullableLowercaseTextSchema,
  medium: nullableLowercaseTextSchema,
  campaign: nullableLowercaseTextSchema,
  content: nullableLowercaseTextSchema,
  term: nullableLowercaseTextSchema,
  click_id_type: z.enum(ATTRIBUTION_CLICK_ID_FIELDS).nullable(),
  click_id_value: nullableTextSchema,
  evidence_source: z.enum(ATTRIBUTION_EVIDENCE_SOURCES),
  is_direct: z.boolean(),
  engagement_type: z.enum(ATTRIBUTION_ENGAGEMENT_TYPES),
  is_synthetic: z.boolean().default(false),
  is_eligible: z.boolean(),
  ineligibility_reason: nullableTextSchema,
  attribution_reason: nullableTextSchema,
  attribution_hint: attributionHintInputV1Schema.nullable()
}).superRefine((value, ctx) => {
  if (!value.is_eligible && !value.ineligibility_reason) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'ineligibility_reason is required when is_eligible is false'
    });
  }
});

export const attributionResultRecordV1Schema = z.object({
  run_id: z.string().uuid(),
  attribution_spec_version: z.literal('v1'),
  order_id: z.string().min(1),
  model_key: z.enum(ATTRIBUTION_MODEL_KEYS),
  allocation_status: z.enum(ATTRIBUTION_ALLOCATION_STATUSES),
  winner_touchpoint_id: nullableTextSchema,
  winner_session_id: uuidOrNullSchema,
  winner_evidence_source: z.enum(ATTRIBUTION_EVIDENCE_SOURCES).nullable(),
  winner_attribution_reason: nullableTextSchema,
  total_credit_weight: decimalStringSchema,
  total_revenue_credited: decimalStringSchema,
  touchpoint_count_considered: z.number().int().nonnegative(),
  eligible_click_count: z.number().int().nonnegative(),
  eligible_view_count: z.number().int().nonnegative(),
  lookback_rule_applied: z.enum(ATTRIBUTION_LOOKBACK_RULES),
  winner_selection_rule: z.enum(ATTRIBUTION_MODEL_KEYS),
  direct_suppression_applied: z.boolean(),
  deterministic_block_applied: z.boolean(),
  normalization_failures_count: z.number().int().nonnegative(),
  generated_at_utc: isoTimestampSchema
});

export const attributionCreditRecordV1Schema = z.object({
  run_id: z.string().uuid(),
  attribution_spec_version: z.literal('v1'),
  order_id: z.string().min(1),
  model_key: z.enum(ATTRIBUTION_MODEL_KEYS),
  touchpoint_id: z.string().min(1).max(MAX_ATTRIBUTION_TEXT_LENGTH),
  session_id: uuidOrNullSchema,
  touchpoint_position: z.number().int().positive(),
  occurred_at_utc: isoTimestampSchema,
  source: nullableLowercaseTextSchema.optional(),
  medium: nullableLowercaseTextSchema.optional(),
  campaign: nullableLowercaseTextSchema.optional(),
  content: nullableLowercaseTextSchema.optional(),
  term: nullableLowercaseTextSchema.optional(),
  click_id_type: z.enum(ATTRIBUTION_CLICK_ID_FIELDS).nullable().optional(),
  click_id_value: nullableTextSchema.optional(),
  touch_type: z.enum(['click', 'view']),
  is_direct: z.boolean(),
  evidence_source: z.enum(ATTRIBUTION_EVIDENCE_SOURCES),
  is_synthetic: z.boolean(),
  attribution_reason: z.string().min(1).max(MAX_ATTRIBUTION_TEXT_LENGTH),
  credit_weight: decimalStringSchema,
  revenue_credit: decimalStringSchema,
  is_primary: z.boolean()
});

export const attributionExplainRecordV1Schema = z.object({
  run_id: z.string().uuid(),
  order_id: z.string().min(1),
  touchpoint_id: nullableTextSchema,
  model_key: z.enum(ATTRIBUTION_MODEL_KEYS).nullable(),
  explain_stage: z.enum(ATTRIBUTION_EXPLAIN_STAGES),
  decision: z.enum(ATTRIBUTION_EXPLAIN_DECISIONS),
  decision_reason: z.string().min(1).max(MAX_ATTRIBUTION_TEXT_LENGTH),
  details_json: z.record(z.string(), z.unknown()),
  order_occurred_at_utc: isoTimestampOrNullSchema,
  created_at_utc: isoTimestampSchema
});

export type AttributionEvidenceSource = (typeof ATTRIBUTION_EVIDENCE_SOURCES)[number];
export type AttributionTouchpointSourceKind = (typeof ATTRIBUTION_TOUCHPOINT_SOURCE_KINDS)[number];
export type AttributionIngestionSource = (typeof ATTRIBUTION_INGESTION_SOURCES)[number];
export type AttributionEngagementType = (typeof ATTRIBUTION_ENGAGEMENT_TYPES)[number];
export type AttributionOrderTimestampSource = (typeof ATTRIBUTION_ORDER_TIMESTAMP_SOURCES)[number];
export type AttributionModelKey = (typeof ATTRIBUTION_MODEL_KEYS)[number];
export type AttributionAllocationStatus = (typeof ATTRIBUTION_ALLOCATION_STATUSES)[number];
export type AttributionLookbackRule = (typeof ATTRIBUTION_LOOKBACK_RULES)[number];
export type AttributionExplainStage = (typeof ATTRIBUTION_EXPLAIN_STAGES)[number];
export type AttributionExplainDecision = (typeof ATTRIBUTION_EXPLAIN_DECISIONS)[number];
export type AttributionHintInputV1 = z.infer<typeof attributionHintInputV1Schema>;
export type AttributionOrderInputV1 = z.infer<typeof attributionOrderInputV1Schema>;
export type AttributionTouchpointInputV1 = z.infer<typeof attributionTouchpointInputV1Schema>;
export type AttributionResultRecordV1 = z.infer<typeof attributionResultRecordV1Schema>;
export type AttributionCreditRecordV1 = z.infer<typeof attributionCreditRecordV1Schema>;
export type AttributionExplainRecordV1 = z.infer<typeof attributionExplainRecordV1Schema>;

export function normalizeAttributionOrderInputV1(input: unknown): AttributionOrderInputV1 {
  return attributionOrderInputV1Schema.parse(input);
}

export function normalizeAttributionTouchpointInputV1(input: unknown): AttributionTouchpointInputV1 {
  return attributionTouchpointInputV1Schema.parse(input);
}

export function normalizeAttributionHintInputV1(input: unknown): AttributionHintInputV1 {
  return attributionHintInputV1Schema.parse(input);
}

export function normalizeAttributionResultRecordV1(input: unknown): AttributionResultRecordV1 {
  return attributionResultRecordV1Schema.parse(input);
}

export function normalizeAttributionCreditRecordV1(input: unknown): AttributionCreditRecordV1 {
  return attributionCreditRecordV1Schema.parse(input);
}

export function normalizeAttributionExplainRecordV1(input: unknown): AttributionExplainRecordV1 {
  return attributionExplainRecordV1Schema.parse(input);
}
