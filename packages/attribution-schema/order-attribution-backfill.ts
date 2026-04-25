import { z } from 'zod';

export const ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT = 500;
export const ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT = 5000;

const ISO_TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;
const DATE_ONLY_REGEX = /^\d{4}-\d{2}-\d{2}$/;

const isoTimestampSchema = z
  .string()
  .trim()
  .refine((value) => ISO_TIMESTAMP_REGEX.test(value), 'Invalid ISO-8601 timestamp')
  .transform((value) => new Date(value).toISOString());

const dateOnlySchema = z.string().trim().regex(DATE_ONLY_REGEX, 'Use YYYY-MM-DD.');

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
