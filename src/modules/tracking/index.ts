import { createHash, randomUUID } from 'node:crypto';

import { type Request, Router } from 'express';
import type { PoolClient } from 'pg';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo, logWarning } from '../../observability/index.js';
import { enqueueAttributionForTrackingTouchpoint } from '../attribution/index.js';
import { buildCanonicalTouchpointDimensions } from '../marketing-dimensions/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { getReportingTimezone, formatDateInTimezone } from '../settings/index.js';

const EVENT_TYPES = ['page_view', 'product_view', 'add_to_cart', 'checkout_started'] as const;
const MAX_URL_LENGTH = 2048;
const MAX_TOKEN_LENGTH = 255;
const MAX_USER_AGENT_LENGTH = 1024;
const MAX_LANGUAGE_LENGTH = 64;
const MAX_SCREEN_LENGTH = 64;
const MAX_CLIENT_EVENT_ID_LENGTH = 128;

class TrackingHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'TrackingHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

type TrackingRequestBody = TrackingEventInput | Record<string, unknown> | string | undefined;

type NormalizedCampaignParameters = Record<
  'utm_source' | 'utm_medium' | 'utm_campaign' | 'utm_content' | 'utm_term' | 'gclid' | 'fbclid' | 'ttclid' | 'msclkid',
  string | null
>;

type TrackingEventInput = z.infer<typeof trackingEventSchema>;

type ExistingTrackingEventRow = {
  id: string;
  occurred_at: Date;
  ingested_at: Date;
  session_id: string;
};

const sanitizedString = (maxLength: number) =>
  z
    .string()
    .trim()
    .max(maxLength)
    .transform((value) => value);

const nullableSanitizedString = (maxLength: number) =>
  z
    .union([sanitizedString(maxLength), z.null(), z.undefined()])
    .transform((value) => {
      if (typeof value !== 'string') {
        return null;
      }

      return value.length > 0 ? value : null;
    });

const trackingEventSchema = z
  .object({
    eventType: z.enum(EVENT_TYPES),
    occurredAt: z.string().datetime(),
    sessionId: z.string().uuid(),
    pageUrl: sanitizedString(MAX_URL_LENGTH),
    referrerUrl: nullableSanitizedString(MAX_URL_LENGTH),
    shopifyCartToken: nullableSanitizedString(MAX_TOKEN_LENGTH),
    shopifyCheckoutToken: nullableSanitizedString(MAX_TOKEN_LENGTH),
    clientEventId: nullableSanitizedString(MAX_CLIENT_EVENT_ID_LENGTH),
    context: z
      .object({
        userAgent: nullableSanitizedString(MAX_USER_AGENT_LENGTH),
        screen: nullableSanitizedString(MAX_SCREEN_LENGTH),
        language: nullableSanitizedString(MAX_LANGUAGE_LENGTH)
      })
      .default({})
  })
  .superRefine((value, ctx) => {
    validateTrackingTimestamp(value.occurredAt, ctx);
    validateHttpUrl(value.pageUrl, 'pageUrl', ctx);

    if (value.referrerUrl) {
      validateHttpUrl(value.referrerUrl, 'referrerUrl', ctx);
    }
  });

function validateTrackingTimestamp(value: string, ctx: z.RefinementCtx): void {
  const occurredAt = new Date(value);
  const now = Date.now();
  const maxAgeMs = env.TRACKING_MAX_EVENT_AGE_HOURS * 60 * 60 * 1000;
  const maxFutureSkewMs = env.TRACKING_MAX_FUTURE_SKEW_SECONDS * 1000;

  if (Number.isNaN(occurredAt.getTime())) {
    return;
  }

  if (occurredAt.getTime() < now - maxAgeMs) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `occurredAt must be within the last ${env.TRACKING_MAX_EVENT_AGE_HOURS} hours`,
      path: ['occurredAt']
    });
  }

  if (occurredAt.getTime() > now + maxFutureSkewMs) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `occurredAt cannot be more than ${env.TRACKING_MAX_FUTURE_SKEW_SECONDS} seconds in the future`,
      path: ['occurredAt']
    });
  }
}

function validateHttpUrl(value: string, field: 'pageUrl' | 'referrerUrl', ctx: z.RefinementCtx): void {
  try {
    const url = new URL(value);

    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `${field} must use http or https`,
        path: [field]
      });
    }
  } catch {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `${field} must be a valid URL`,
      path: [field]
    });
  }
}

function normalizeUrl(rawUrl: string | null): string | null {
  if (!rawUrl) {
    return null;
  }

  const url = new URL(rawUrl);
  url.hash = '';
  return url.toString();
}

function parseCampaignParameters(pageUrl: string): NormalizedCampaignParameters {
  const url = new URL(pageUrl);

  return {
    utm_source: normalizeNullableString(url.searchParams.get('utm_source')),
    utm_medium: normalizeNullableString(url.searchParams.get('utm_medium')),
    utm_campaign: normalizeNullableString(url.searchParams.get('utm_campaign')),
    utm_content: normalizeNullableString(url.searchParams.get('utm_content')),
    utm_term: normalizeNullableString(url.searchParams.get('utm_term')),
    gclid: normalizeNullableString(url.searchParams.get('gclid')),
    fbclid: normalizeNullableString(url.searchParams.get('fbclid')),
    ttclid: normalizeNullableString(url.searchParams.get('ttclid')),
    msclkid: normalizeNullableString(url.searchParams.get('msclkid'))
  };
}

function hashIp(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  return createHash('sha256').update(value).digest('hex');
}

function normalizeNullableString(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function hashTrackingFingerprint(input: TrackingEventInput): string {
  const fingerprintSource = JSON.stringify({
    sessionId: input.sessionId,
    eventType: input.eventType,
    occurredAt: new Date(input.occurredAt).toISOString(),
    pageUrl: normalizeUrl(input.pageUrl),
    referrerUrl: normalizeUrl(input.referrerUrl),
    shopifyCartToken: normalizeNullableString(input.shopifyCartToken),
    shopifyCheckoutToken: normalizeNullableString(input.shopifyCheckoutToken)
  });

  return createHash('sha256').update(fingerprintSource).digest('hex');
}

function sanitizeTrackingInput(input: TrackingEventInput): TrackingEventInput {
  return {
    ...input,
    occurredAt: new Date(input.occurredAt).toISOString(),
    pageUrl: normalizeUrl(input.pageUrl) ?? input.pageUrl,
    referrerUrl: normalizeUrl(input.referrerUrl),
    shopifyCartToken: normalizeNullableString(input.shopifyCartToken),
    shopifyCheckoutToken: normalizeNullableString(input.shopifyCheckoutToken),
    clientEventId: normalizeNullableString(input.clientEventId),
    context: {
      userAgent: normalizeNullableString(input.context.userAgent),
      screen: normalizeNullableString(input.context.screen),
      language: normalizeNullableString(input.context.language)
    }
  };
}

async function findExistingTrackingEventByClientEventId(clientEventId: string): Promise<ExistingTrackingEventRow | null> {
  const result = await query<ExistingTrackingEventRow>(
    `
      SELECT
        id,
        occurred_at,
        ingested_at,
        session_id
      FROM tracking_events
      WHERE client_event_id = $1
      LIMIT 1
    `,
    [clientEventId]
  );

  return result.rows[0] ?? null;
}

async function findExistingTrackingEventByFingerprint(ingestionFingerprint: string): Promise<ExistingTrackingEventRow | null> {
  const result = await query<ExistingTrackingEventRow>(
    `
      SELECT
        id,
        occurred_at,
        ingested_at,
        session_id
      FROM tracking_events
      WHERE ingestion_fingerprint = $1
      LIMIT 1
    `,
    [ingestionFingerprint]
  );

  return result.rows[0] ?? null;
}

async function upsertTrackingSession(
  input: TrackingEventInput,
  occurredAt: Date,
  userAgent: string | null,
  ipHash: string | null,
  client: PoolClient
): Promise<void> {
  const params = parseCampaignParameters(input.pageUrl);
  const canonicalDimensions = buildCanonicalTouchpointDimensions({
    source: params.utm_source,
    medium: params.utm_medium,
    campaign: params.utm_campaign,
    content: params.utm_content,
    term: params.utm_term,
    gclid: params.gclid,
    fbclid: params.fbclid,
    ttclid: params.ttclid,
    msclkid: params.msclkid
  });

  await client.query(
    `
      INSERT INTO tracking_sessions (
        id,
        created_at,
        updated_at,
        first_seen_at,
        last_seen_at,
        landing_page,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_fbclid,
        initial_ttclid,
        initial_msclkid,
        user_agent,
        ip_hash
      )
      VALUES (
        $1,
        now(),
        now(),
        $2,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15
      )
      ON CONFLICT (id)
      DO UPDATE SET
        updated_at = now(),
        last_seen_at = GREATEST(tracking_sessions.last_seen_at, EXCLUDED.last_seen_at),
        landing_page = COALESCE(tracking_sessions.landing_page, EXCLUDED.landing_page),
        referrer_url = COALESCE(tracking_sessions.referrer_url, EXCLUDED.referrer_url),
        initial_utm_source = COALESCE(tracking_sessions.initial_utm_source, EXCLUDED.initial_utm_source),
        initial_utm_medium = COALESCE(tracking_sessions.initial_utm_medium, EXCLUDED.initial_utm_medium),
        initial_utm_campaign = COALESCE(tracking_sessions.initial_utm_campaign, EXCLUDED.initial_utm_campaign),
        initial_utm_content = COALESCE(tracking_sessions.initial_utm_content, EXCLUDED.initial_utm_content),
        initial_utm_term = COALESCE(tracking_sessions.initial_utm_term, EXCLUDED.initial_utm_term),
        initial_gclid = COALESCE(tracking_sessions.initial_gclid, EXCLUDED.initial_gclid),
        initial_fbclid = COALESCE(tracking_sessions.initial_fbclid, EXCLUDED.initial_fbclid),
        initial_ttclid = COALESCE(tracking_sessions.initial_ttclid, EXCLUDED.initial_ttclid),
        initial_msclkid = COALESCE(tracking_sessions.initial_msclkid, EXCLUDED.initial_msclkid),
        user_agent = COALESCE(tracking_sessions.user_agent, EXCLUDED.user_agent),
        ip_hash = COALESCE(tracking_sessions.ip_hash, EXCLUDED.ip_hash)
    `,
    [
      input.sessionId,
      occurredAt,
      input.pageUrl,
      input.referrerUrl ?? null,
      canonicalDimensions.source,
      canonicalDimensions.medium,
      canonicalDimensions.campaign,
      canonicalDimensions.content,
      canonicalDimensions.term,
      canonicalDimensions.clickIdType === 'gclid' ? canonicalDimensions.clickIdValue : null,
      canonicalDimensions.clickIdType === 'fbclid' ? canonicalDimensions.clickIdValue : null,
      canonicalDimensions.clickIdType === 'ttclid' ? canonicalDimensions.clickIdValue : null,
      canonicalDimensions.clickIdType === 'msclkid' ? canonicalDimensions.clickIdValue : null,
      userAgent,
      ipHash
    ]
  );
}

async function insertTrackingEvent(
  client: PoolClient,
  input: TrackingEventInput,
  ingestionFingerprint: string
): Promise<string> {
  const params = parseCampaignParameters(input.pageUrl);
  const canonicalDimensions = buildCanonicalTouchpointDimensions({
    source: params.utm_source,
    medium: params.utm_medium,
    campaign: params.utm_campaign,
    content: params.utm_content,
    term: params.utm_term,
    gclid: params.gclid,
    fbclid: params.fbclid,
    ttclid: params.ttclid,
    msclkid: params.msclkid
  });
  const eventId = randomUUID();

  try {
    await client.query(
      `
        INSERT INTO tracking_events (
          id,
          session_id,
          event_type,
          occurred_at,
          page_url,
          referrer_url,
          utm_source,
          utm_medium,
          utm_campaign,
          utm_content,
          utm_term,
          gclid,
          fbclid,
          ttclid,
          msclkid,
          shopify_cart_token,
          shopify_checkout_token,
          client_event_id,
          ingestion_fingerprint,
          ingested_at,
          raw_payload
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7,
          $8,
          $9,
          $10,
          $11,
          $12,
          $13,
          $14,
          $15,
          $16,
          $17,
          $18,
          $19,
          now(),
          $20::jsonb
        )
      `,
      [
        eventId,
        input.sessionId,
        input.eventType,
        new Date(input.occurredAt),
        input.pageUrl,
        input.referrerUrl ?? null,
        canonicalDimensions.source,
        canonicalDimensions.medium,
        canonicalDimensions.campaign,
        canonicalDimensions.content,
        canonicalDimensions.term,
        canonicalDimensions.clickIdType === 'gclid' ? canonicalDimensions.clickIdValue : null,
        canonicalDimensions.clickIdType === 'fbclid' ? canonicalDimensions.clickIdValue : null,
        canonicalDimensions.clickIdType === 'ttclid' ? canonicalDimensions.clickIdValue : null,
        canonicalDimensions.clickIdType === 'msclkid' ? canonicalDimensions.clickIdValue : null,
        normalizeNullableString(input.shopifyCartToken),
        normalizeNullableString(input.shopifyCheckoutToken),
        input.clientEventId ?? null,
        ingestionFingerprint,
        JSON.stringify({
          ...input,
          marketingDimensions: canonicalDimensions
        })
      ]
    );
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
      const existing =
        (input.clientEventId ? await findExistingTrackingEventByClientEventId(input.clientEventId) : null) ??
        (await findExistingTrackingEventByFingerprint(ingestionFingerprint));

      if (existing) {
        return existing.id;
      }
    }

    throw error;
  }

  return eventId;
}

function resolveRequestIp(req: Request): string | undefined {
  const forwardedFor = req.header('x-forwarded-for');

  if (forwardedFor) {
    return forwardedFor.split(',')[0]?.trim();
  }

  return req.ip;
}

type RateLimitEntry = {
  count: number;
  resetAt: number;
};

class InMemoryRateLimiter {
  private readonly entries = new Map<string, RateLimitEntry>();

  check(key: string): { allowed: boolean; remaining: number; resetAt: number } {
    const now = Date.now();
    const existing = this.entries.get(key);

    if (!existing || existing.resetAt <= now) {
      const nextEntry = {
        count: 1,
        resetAt: now + env.TRACKING_RATE_LIMIT_WINDOW_MS
      };

      this.entries.set(key, nextEntry);
      this.prune(now);

      return {
        allowed: true,
        remaining: Math.max(env.TRACKING_RATE_LIMIT_MAX - nextEntry.count, 0),
        resetAt: nextEntry.resetAt
      };
    }

    existing.count += 1;
    this.entries.set(key, existing);

    return {
      allowed: existing.count <= env.TRACKING_RATE_LIMIT_MAX,
      remaining: Math.max(env.TRACKING_RATE_LIMIT_MAX - existing.count, 0),
      resetAt: existing.resetAt
    };
  }

  private prune(now: number): void {
    if (this.entries.size <= 10_000) {
      return;
    }

    for (const [key, entry] of this.entries.entries()) {
      if (entry.resetAt <= now) {
        this.entries.delete(key);
      }
    }
  }
}

const trackingRateLimiter = new InMemoryRateLimiter();

function enforceAllowedOrigin(req: Request): void {
  if (!env.TRACKING_ALLOWED_ORIGINS.length) {
    return;
  }

  const origin = req.header('origin');

  if (!origin || !env.TRACKING_ALLOWED_ORIGINS.includes(origin)) {
    throw new TrackingHttpError(403, 'origin_not_allowed', 'Request origin is not allowed', {
      allowedOrigins: env.TRACKING_ALLOWED_ORIGINS
    });
  }
}

function enforceSupportedContentType(req: Request): void {
  if (!req.is(['application/json', 'text/plain'])) {
    throw new TrackingHttpError(
      415,
      'unsupported_media_type',
      'Tracking endpoint requires application/json or text/plain JSON'
    );
  }
}

function parseTrackingRequestBody(body: TrackingRequestBody): Record<string, unknown> {
  if (typeof body === 'string') {
    const trimmed = body.trim();

    if (!trimmed) {
      throw new TrackingHttpError(400, 'invalid_json', 'Request body must not be empty');
    }

    try {
      const parsed = JSON.parse(trimmed) as unknown;

      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new TrackingHttpError(400, 'invalid_json', 'Tracking payload must be a JSON object');
      }

      return parsed as Record<string, unknown>;
    } catch (error) {
      if (error instanceof TrackingHttpError) {
        throw error;
      }

      throw new TrackingHttpError(400, 'invalid_json', 'Request body must be valid JSON');
    }
  }

  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    throw new TrackingHttpError(400, 'invalid_json', 'Tracking payload must be a JSON object');
  }

  return body;
}

function enforceRateLimit(req: Request): void {
  const requestIp = resolveRequestIp(req);
  const sessionHint = typeof req.body?.sessionId === 'string' ? req.body.sessionId : 'anonymous';
  const key = createHash('sha256').update(`${requestIp ?? 'unknown'}:${sessionHint}`).digest('hex');
  const result = trackingRateLimiter.check(key);

  if (!result.allowed) {
    throw new TrackingHttpError(429, 'rate_limit_exceeded', 'Too many tracking requests', {
      retryAfterMs: Math.max(result.resetAt - Date.now(), 0)
    });
  }
}

async function ingestTrackingEvent(
  input: TrackingEventInput,
  requestIp: string | undefined
): Promise<{
  eventId: string;
  ingestedAt: string;
  sessionId: string;
  deduplicated: boolean;
}> {
  const sanitizedInput = sanitizeTrackingInput(input);
  const ipHash = hashIp(requestIp);
  const ingestionFingerprint = hashTrackingFingerprint(sanitizedInput);

  if (sanitizedInput.clientEventId) {
    const existing = await findExistingTrackingEventByClientEventId(sanitizedInput.clientEventId);

    if (existing) {
      return {
        eventId: existing.id,
        ingestedAt: existing.ingested_at.toISOString(),
        sessionId: existing.session_id,
        deduplicated: true
      };
    }
  }

  const duplicatePayload = await findExistingTrackingEventByFingerprint(ingestionFingerprint);

  if (duplicatePayload) {
    return {
      eventId: duplicatePayload.id,
      ingestedAt: duplicatePayload.ingested_at.toISOString(),
      sessionId: duplicatePayload.session_id,
      deduplicated: true
    };
  }

  const eventId = await withTransaction(async (client) => {
    const occurredAt = new Date(sanitizedInput.occurredAt);
    const userAgent = sanitizedInput.context.userAgent ?? null;
    const metricDate = formatDateInTimezone(occurredAt, await getReportingTimezone(client));

    await upsertTrackingSession(sanitizedInput, occurredAt, userAgent, ipHash, client);
    const insertedEventId = await insertTrackingEvent(client, sanitizedInput, ingestionFingerprint);
    await enqueueAttributionForTrackingTouchpoint(client, {
      sessionId: sanitizedInput.sessionId,
      shopifyCheckoutToken: normalizeNullableString(sanitizedInput.shopifyCheckoutToken),
      shopifyCartToken: normalizeNullableString(sanitizedInput.shopifyCartToken)
    });
    await refreshDailyReportingMetrics(client, [metricDate]);

    return insertedEventId;
  });

  return {
    eventId,
    ingestedAt: new Date().toISOString(),
    sessionId: sanitizedInput.sessionId,
    deduplicated: false
  };
}

export function createTrackingRouter(): Router {
  const router = Router();

  router.post('/', async (req, res, next) => {
    try {
      enforceSupportedContentType(req);
      enforceAllowedOrigin(req);
      enforceRateLimit(req);

      const body = parseTrackingRequestBody(req.body as TrackingRequestBody);
      const input = trackingEventSchema.parse(body);
      const result = await ingestTrackingEvent(input, resolveRequestIp(req));
      logInfo(result.deduplicated ? 'tracking_ingest_duplicate' : 'tracking_ingest_accepted', {
        sessionId: result.sessionId,
        eventId: result.eventId,
        eventType: input.eventType,
        deduplicated: result.deduplicated
      });

      res.status(200).json({
        ok: true,
        eventId: result.eventId,
        ingestedAt: result.ingestedAt,
        sessionId: result.sessionId
      });
    } catch (error) {
      if (error instanceof TrackingHttpError) {
        logWarning('tracking_ingest_rejected', {
          code: error.code,
          statusCode: error.statusCode,
          details: error.details
        });
      } else {
        logError('tracking_ingest_failed', error, {
          path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path
        });
      }
      next(error);
    }
  });

  return router;
}
