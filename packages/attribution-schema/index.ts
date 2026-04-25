import { z } from 'zod';

export const ATTRIBUTION_SCHEMA_VERSION = 1 as const;
export const MAX_ATTRIBUTION_URL_LENGTH = 2048;
export const MAX_ATTRIBUTION_TEXT_LENGTH = 255;
export const MAX_SESSION_ID_LENGTH = 36;
export const ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT = 500;
export const ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT = 5000;
export const ATTRIBUTION_CONSENT_STATES = ['granted', 'denied', 'unknown'] as const;
export const ATTRIBUTION_URL_FIELDS = ['landing_url', 'referrer_url', 'page_url'] as const;
export const ATTRIBUTION_UTM_FIELDS = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'] as const;
export const ATTRIBUTION_CLICK_ID_FIELDS = ['gclid', 'gbraid', 'wbraid', 'fbclid', 'ttclid', 'msclkid'] as const;

const ISO_TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const DATE_ONLY_REGEX = /^\d{4}-\d{2}-\d{2}$/;

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

const dateOnlySchema = z.string().trim().regex(DATE_ONLY_REGEX, 'Use YYYY-MM-DD.');

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

export const orderAttributionBackfillRequestSchema = z
  .object({
    startDate: dateOnlySchema,
    endDate: dateOnlySchema,
    dryRun: z.boolean().default(true),
    limit: z
      .number()
      .int('Limit must be a whole number.')
      .positive('Limit must be greater than 0.')
      .max(ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT, `Limit must be ${ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT} or less.`)
      .default(ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT),
    webOrdersOnly: z.boolean().default(true),
    skipShopifyWriteback: z.boolean().default(false)
  })
  .refine((value) => value.startDate <= value.endDate, {
    message: 'Start date must be on or before end date.',
    path: ['endDate']
  });

export const orderAttributionBackfillSubmittedOptionsSchema = orderAttributionBackfillRequestSchema;

export const orderAttributionBackfillJobStatusSchema = z.enum(['queued', 'processing', 'completed', 'failed']);

export const orderAttributionBackfillFailureSchema = z.object({
  orderId: z.string().nullable(),
  code: z.string(),
  message: z.string()
});

export const orderAttributionBackfillReportSchema = z.object({
  scanned: z.number().int().nonnegative(),
  recovered: z.number().int().nonnegative(),
  unrecoverable: z.number().int().nonnegative(),
  writebackCompleted: z.number().int().nonnegative(),
  failures: z.array(orderAttributionBackfillFailureSchema)
});

export const orderAttributionBackfillEnqueueResponseSchema = z.object({
  ok: z.literal(true),
  jobId: z.string(),
  status: orderAttributionBackfillJobStatusSchema,
  submittedAt: isoTimestampSchema,
  submittedBy: z.string(),
  options: orderAttributionBackfillSubmittedOptionsSchema
});

export const orderAttributionBackfillJobResponseSchema = z.object({
  ok: z.literal(true),
  jobId: z.string(),
  status: orderAttributionBackfillJobStatusSchema,
  submittedAt: isoTimestampSchema,
  submittedBy: z.string(),
  startedAt: isoTimestampSchema.nullable(),
  completedAt: isoTimestampSchema.nullable(),
  options: orderAttributionBackfillSubmittedOptionsSchema,
  report: orderAttributionBackfillReportSchema.nullable(),
  error: z
    .object({
      code: z.string(),
      message: z.string()
    })
    .nullable()
});

export type AttributionSchemaVersion = typeof ATTRIBUTION_SCHEMA_VERSION;
export type AttributionUrlField = (typeof ATTRIBUTION_URL_FIELDS)[number];
export type AttributionUtmField = (typeof ATTRIBUTION_UTM_FIELDS)[number];
export type AttributionClickIdField = (typeof ATTRIBUTION_CLICK_ID_FIELDS)[number];
export type AttributionCaptureV1 = z.infer<typeof attributionCaptureV1Schema>;
export type AttributionConsentState = z.infer<typeof attributionConsentStateSchema>;
export type OrderAttributionBackfillRequest = z.infer<typeof orderAttributionBackfillRequestSchema>;
export type OrderAttributionBackfillSubmittedOptions = z.infer<typeof orderAttributionBackfillSubmittedOptionsSchema>;
export type OrderAttributionBackfillJobStatus = z.infer<typeof orderAttributionBackfillJobStatusSchema>;
export type OrderAttributionBackfillFailure = z.infer<typeof orderAttributionBackfillFailureSchema>;
export type OrderAttributionBackfillReport = z.infer<typeof orderAttributionBackfillReportSchema>;
export type OrderAttributionBackfillEnqueueResponse = z.infer<typeof orderAttributionBackfillEnqueueResponseSchema>;
export type OrderAttributionBackfillJobResponse = z.infer<typeof orderAttributionBackfillJobResponseSchema>;

export function normalizeAttributionCaptureV1(input: unknown): AttributionCaptureV1 {
  return attributionCaptureV1Schema.parse(input);
}

export function normalizeAttributionConsentState(input: unknown): AttributionConsentState {
  return attributionConsentStateSchema.parse(input);
}

export function normalizeOrderAttributionBackfillRequest(input: unknown): OrderAttributionBackfillRequest {
  return orderAttributionBackfillRequestSchema.parse(input);
}
