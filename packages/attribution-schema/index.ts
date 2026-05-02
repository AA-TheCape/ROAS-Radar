import { z } from 'zod';

export {
  ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
  ORDER_ATTRIBUTION_BACKFILL_MAX_ORGANIZATION_IDS,
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
