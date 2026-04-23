import { z } from 'zod';

export const ATTRIBUTION_SCHEMA_VERSION = 1 as const;
export const MAX_ATTRIBUTION_URL_LENGTH = 2048;
export const MAX_ATTRIBUTION_TEXT_LENGTH = 255;
export const MAX_SESSION_ID_LENGTH = 36;
export const ATTRIBUTION_CONSENT_STATES = ['granted', 'denied', 'unknown'] as const;

const ISO_TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;

type NullableString = string | null | undefined;

function normalizeNullableString(value: NullableString): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function normalizeNullableLowercaseString(value: NullableString): string | null {
  const normalized = normalizeNullableString(value);
  return normalized ? normalized.toLowerCase() : null;
}

function normalizeUrl(value: NullableString): string | null {
  const normalized = normalizeNullableString(value);

  if (!normalized) {
    return null;
  }

  const url = new URL(normalized);

  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error('invalid_protocol');
  }

  url.hash = '';
  return url.toString();
}

const nullableUrlSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeNullableString(value))
  .refine((value) => value === null || value.length <= MAX_ATTRIBUTION_URL_LENGTH, {
    message: `String must contain at most ${MAX_ATTRIBUTION_URL_LENGTH} character(s)`
  })
  .superRefine((value, ctx) => {
    if (!value) {
      return;
    }

    try {
      normalizeUrl(value);
    } catch (error) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: error instanceof Error && error.message === 'invalid_protocol' ? 'URL must use http or https' : 'Invalid URL'
      });
    }
  })
  .transform((value) => (value ? normalizeUrl(value) : null));

const nullableUtmSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeNullableString(value))
  .refine((value) => value === null || value.length <= MAX_ATTRIBUTION_TEXT_LENGTH, {
    message: `String must contain at most ${MAX_ATTRIBUTION_TEXT_LENGTH} character(s)`
  })
  .transform((value) => normalizeNullableLowercaseString(value));

const nullableClickIdSchema = z
  .union([z.string(), z.null(), z.undefined()])
  .transform((value) => normalizeNullableString(value))
  .refine((value) => value === null || value.length <= MAX_ATTRIBUTION_TEXT_LENGTH, {
    message: `String must contain at most ${MAX_ATTRIBUTION_TEXT_LENGTH} character(s)`
  });

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
export type AttributionCaptureV1 = z.infer<typeof attributionCaptureV1Schema>;
export type AttributionConsentState = z.infer<typeof attributionConsentStateSchema>;

export function normalizeAttributionCaptureV1(input: unknown): AttributionCaptureV1 {
  return attributionCaptureV1Schema.parse(input);
}

export function normalizeAttributionConsentState(input: unknown): AttributionConsentState {
  return attributionConsentStateSchema.parse(input);
}
