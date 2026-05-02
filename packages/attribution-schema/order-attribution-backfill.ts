import { z } from 'zod';

export const ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT = 500;
export const ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT = 5000;
export const ORDER_ATTRIBUTION_BACKFILL_MAX_ORGANIZATION_IDS = 100;

const ISO_TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;
const DATE_ONLY_REGEX = /^\d{4}-\d{2}-\d{2}$/;

const isoTimestampSchema = z
  .string()
  .trim()
  .refine((value) => ISO_TIMESTAMP_REGEX.test(value), 'Invalid ISO-8601 timestamp')
  .transform((value) => new Date(value).toISOString());

const dateOnlySchema = z.string().trim().regex(DATE_ONLY_REGEX, 'Use YYYY-MM-DD.');
const orderAttributionTierSchema = z.enum([
  'deterministic_first_party',
  'deterministic_shopify_hint',
  'platform_reported_meta',
  'ga4_fallback',
  'unattributed'
]);
const backfillTargetSchema = z.enum(['full_rebuild', 'meta_tier_reclassification']);
const organizationIdsSchema = z
  .array(z.coerce.number().int().positive('Organization ids must be positive integers.'))
  .max(
    ORDER_ATTRIBUTION_BACKFILL_MAX_ORGANIZATION_IDS,
    `Organization ids must contain ${ORDER_ATTRIBUTION_BACKFILL_MAX_ORGANIZATION_IDS} entries or less.`
  )
  .transform((value) => Array.from(new Set(value)).sort((left, right) => left - right))
  .default([]);

const attributionTierCountsSchema = z.object({
  deterministic_first_party: z.number().int().nonnegative(),
  deterministic_shopify_hint: z.number().int().nonnegative(),
  platform_reported_meta: z.number().int().nonnegative(),
  ga4_fallback: z.number().int().nonnegative(),
  unattributed: z.number().int().nonnegative()
});
const emptyAttributionTierCounts = {
  deterministic_first_party: 0,
  deterministic_shopify_hint: 0,
  platform_reported_meta: 0,
  ga4_fallback: 0,
  unattributed: 0
} as const;

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
    reclassificationTarget: backfillTargetSchema.default('full_rebuild'),
    organizationIds: organizationIdsSchema,
    webOrdersOnly: z.boolean().default(true),
    skipShopifyWriteback: z.boolean().default(false),
    idempotencyKey: z.string().trim().min(1).max(255).optional()
  })
  .refine((value) => value.startDate <= value.endDate, {
    message: 'Start date must be on or before end date.',
    path: ['endDate']
  })
  .superRefine((value, ctx) => {
    if (value.organizationIds.length > 0 && value.reclassificationTarget !== 'meta_tier_reclassification') {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Organization ids may only be provided for Meta tier reclassification backfills.',
        path: ['organizationIds']
      });
    }
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
  dryRun: z.boolean().default(false),
  reclassificationTarget: backfillTargetSchema.default('full_rebuild'),
  organizationIds: organizationIdsSchema,
  beforeCounts: attributionTierCountsSchema.default(emptyAttributionTierCounts),
  afterCounts: attributionTierCountsSchema.default(emptyAttributionTierCounts),
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

export type OrderAttributionBackfillRequest = z.infer<typeof orderAttributionBackfillRequestSchema>;
export type OrderAttributionBackfillSubmittedOptions = z.infer<typeof orderAttributionBackfillSubmittedOptionsSchema>;
export type OrderAttributionBackfillJobStatus = z.infer<typeof orderAttributionBackfillJobStatusSchema>;
export type OrderAttributionBackfillFailure = z.infer<typeof orderAttributionBackfillFailureSchema>;
export type OrderAttributionBackfillReport = z.infer<typeof orderAttributionBackfillReportSchema>;
export type OrderAttributionBackfillEnqueueResponse = z.infer<typeof orderAttributionBackfillEnqueueResponseSchema>;
export type OrderAttributionBackfillJobResponse = z.infer<typeof orderAttributionBackfillJobResponseSchema>;

export function normalizeOrderAttributionBackfillRequest(input: unknown): OrderAttributionBackfillRequest {
  return orderAttributionBackfillRequestSchema.parse(input);
}
