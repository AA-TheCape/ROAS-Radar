import { createHash, randomUUID } from 'node:crypto';

import { type Request, type Response, Router } from 'express';
import type { PoolClient } from 'pg';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo, logWarning } from '../../observability/index.js';
import {
  ATTRIBUTION_SCHEMA_VERSION,
  ATTRIBUTION_CONSENT_STATES,
  normalizeAttributionConsentState,
  normalizeAttributionCaptureV1,
  type AttributionCaptureV1,
  type AttributionConsentState
} from '../../../packages/attribution-schema/index.js';
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
const TRACKING_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365;
const TRACKING_COOKIE_NAMES = ['_hba_id', 'roas_radar_session_id'] as const;
const REQUEST_CONTEXT_INGESTION_SOURCES = ['request_query', 'request_referrer'] as const;
const FALLBACK_BROWSER_DEDUPLICATION_WINDOW_MINUTES = 5;

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
  | 'utm_source'
  | 'utm_medium'
  | 'utm_campaign'
  | 'utm_content'
  | 'utm_term'
  | 'gclid'
  | 'gbraid'
  | 'wbraid'
  | 'fbclid'
  | 'ttclid'
  | 'msclkid',
  string | null
>;

type SessionBootstrapInput = z.infer<typeof sessionBootstrapSchema>;

type TrackingEventInput = z.infer<typeof trackingEventSchema>;

type TrackingIngestionSource = 'browser' | 'server' | (typeof REQUEST_CONTEXT_INGESTION_SOURCES)[number];

type RequestContextAttributionCapture = {
  input: AttributionCaptureV1;
  consentState: AttributionConsentState;
  ingestionSource: (typeof REQUEST_CONTEXT_INGESTION_SOURCES)[number];
};

type AttributionCaptureInsertOptions = {
  consentState: AttributionConsentState;
  eventType: string;
  ingestionSource: TrackingIngestionSource;
  deduplicationKey: string;
  mirrorTrackingEvent?: boolean;
  rawPayload: unknown;
};

type ExistingTrackingEventRow = {
  id: string;
  occurred_at: Date;
  ingested_at: Date;
  session_id: string;
};

type SessionIdentityRow = {
  roas_radar_session_id: string;
  created_at: Date;
};

type ExistingSessionAttributionTouchEventRow = {
  id: number;
  captured_at: Date;
  roas_radar_session_id: string;
};

type ServerAttributionResult = {
  ok: boolean;
  touchEventId: number | null;
  trackingEventId: string | null;
  capturedAt: string | null;
  deduplicated: boolean;
  errorCode: string | null;
};

type BrowserTrackingIngestResult = {
  eventId: string;
  ingestedAt: string;
  sessionId: string;
  deduplicated: boolean;
  sanitizedInput: TrackingEventInput;
  deduplicationKey: string;
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
    consentState: z.enum(ATTRIBUTION_CONSENT_STATES).default('unknown'),
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

const sessionBootstrapSchema = z
  .object({
    pageUrl: nullableSanitizedString(MAX_URL_LENGTH),
    landingUrl: nullableSanitizedString(MAX_URL_LENGTH),
    referrerUrl: nullableSanitizedString(MAX_URL_LENGTH)
  })
  .superRefine((value, ctx) => {
    if (value.pageUrl) {
      validateHttpUrl(value.pageUrl, 'pageUrl', ctx);
    }

    if (value.referrerUrl) {
      validateHttpUrl(value.referrerUrl, 'referrerUrl', ctx);
    }

    if (value.landingUrl) {
      try {
        const url = new URL(value.landingUrl);

        if (url.protocol !== 'http:' && url.protocol !== 'https:') {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'landingUrl must use http or https',
            path: ['landingUrl']
          });
        }
      } catch {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'landingUrl must be a valid URL',
          path: ['landingUrl']
        });
      }
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

function normalizeNullableUrl(rawUrl: string | null | undefined): string | null {
  return normalizeUrl(normalizeNullableString(rawUrl));
}

function readRequestCookies(req: Request): Record<string, string> {
  const header = req.header('cookie');

  if (!header) {
    return {};
  }

  return header.split(';').reduce<Record<string, string>>((cookies, entry) => {
    const separatorIndex = entry.indexOf('=');

    if (separatorIndex <= 0) {
      return cookies;
    }

    const name = entry.slice(0, separatorIndex).trim();
    const value = entry.slice(separatorIndex + 1).trim();

    if (!name) {
      return cookies;
    }

    try {
      cookies[name] = decodeURIComponent(value);
    } catch {
      cookies[name] = value;
    }

    return cookies;
  }, {});
}

function resolveSessionIdFromRequest(req: Request): string | null {
  const cookies = readRequestCookies(req);

  for (const cookieName of TRACKING_COOKIE_NAMES) {
    const value = normalizeNullableString(cookies[cookieName]);

    if (value && z.string().uuid().safeParse(value).success) {
      return value;
    }
  }

  return null;
}

function appendTrackingSessionCookies(req: Request, res: Response, sessionId: string): void {
  const isSecureRequest = req.secure || req.header('x-forwarded-proto') === 'https';

  for (const cookieName of TRACKING_COOKIE_NAMES) {
    res.append(
      'Set-Cookie',
      [
        `${cookieName}=${encodeURIComponent(sessionId)}`,
        `Max-Age=${TRACKING_COOKIE_MAX_AGE_SECONDS}`,
        'Path=/',
        'SameSite=Lax',
        ...(isSecureRequest ? ['Secure'] : [])
      ].join('; ')
    );
  }
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
    gbraid: normalizeNullableString(url.searchParams.get('gbraid')),
    wbraid: normalizeNullableString(url.searchParams.get('wbraid')),
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

function buildTrackingAttributionCapture(input: TrackingEventInput, capturedAt: string): AttributionCaptureV1 {
  const campaignParameters = parseCampaignParameters(input.pageUrl);

  return normalizeAttributionCaptureV1({
    schema_version: ATTRIBUTION_SCHEMA_VERSION,
    roas_radar_session_id: input.sessionId,
    occurred_at: input.occurredAt,
    captured_at: capturedAt,
    landing_url: input.pageUrl,
    referrer_url: input.referrerUrl,
    page_url: input.pageUrl,
    utm_source: campaignParameters.utm_source,
    utm_medium: campaignParameters.utm_medium,
    utm_campaign: campaignParameters.utm_campaign,
    utm_content: campaignParameters.utm_content,
    utm_term: campaignParameters.utm_term,
    gclid: campaignParameters.gclid,
    gbraid: campaignParameters.gbraid,
    wbraid: campaignParameters.wbraid,
    fbclid: campaignParameters.fbclid,
    ttclid: campaignParameters.ttclid,
    msclkid: campaignParameters.msclkid
  });
}

function buildUnifiedEventFingerprintSource(input: {
  sessionId: string;
  eventType: string;
  occurredAt: string;
  pageUrl: string | null;
  referrerUrl: string | null;
  utmSource: string | null;
  utmMedium: string | null;
  utmCampaign: string | null;
  utmContent: string | null;
  utmTerm: string | null;
  gclid: string | null;
  gbraid: string | null;
  wbraid: string | null;
  fbclid: string | null;
  ttclid: string | null;
  msclkid: string | null;
  shopifyCartToken?: string | null;
  shopifyCheckoutToken?: string | null;
}): string {
  return JSON.stringify({
    sessionId: input.sessionId,
    eventType: input.eventType,
    occurredAt: new Date(input.occurredAt).toISOString(),
    pageUrl: normalizeUrl(input.pageUrl),
    referrerUrl: normalizeUrl(input.referrerUrl),
    utmSource: normalizeNullableString(input.utmSource),
    utmMedium: normalizeNullableString(input.utmMedium),
    utmCampaign: normalizeNullableString(input.utmCampaign),
    utmContent: normalizeNullableString(input.utmContent),
    utmTerm: normalizeNullableString(input.utmTerm),
    gclid: normalizeNullableString(input.gclid),
    gbraid: normalizeNullableString(input.gbraid),
    wbraid: normalizeNullableString(input.wbraid),
    fbclid: normalizeNullableString(input.fbclid),
    ttclid: normalizeNullableString(input.ttclid),
    msclkid: normalizeNullableString(input.msclkid),
    shopifyCartToken: normalizeNullableString(input.shopifyCartToken),
    shopifyCheckoutToken: normalizeNullableString(input.shopifyCheckoutToken)
  });
}

function hashTrackingFingerprint(input: TrackingEventInput): string {
  const campaignParameters = parseCampaignParameters(input.pageUrl);
  const fingerprintSource = buildUnifiedEventFingerprintSource({
    sessionId: input.sessionId,
    eventType: input.eventType,
    occurredAt: input.occurredAt,
    pageUrl: input.pageUrl,
    referrerUrl: input.referrerUrl,
    utmSource: campaignParameters.utm_source,
    utmMedium: campaignParameters.utm_medium,
    utmCampaign: campaignParameters.utm_campaign,
    utmContent: campaignParameters.utm_content,
    utmTerm: campaignParameters.utm_term,
    gclid: campaignParameters.gclid,
    gbraid: campaignParameters.gbraid,
    wbraid: campaignParameters.wbraid,
    fbclid: campaignParameters.fbclid,
    ttclid: campaignParameters.ttclid,
    msclkid: campaignParameters.msclkid,
    shopifyCartToken: input.shopifyCartToken,
    shopifyCheckoutToken: input.shopifyCheckoutToken
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
    consentState: input.consentState,
    context: {
      userAgent: normalizeNullableString(input.context.userAgent),
      screen: normalizeNullableString(input.context.screen),
      language: normalizeNullableString(input.context.language)
    }
  };
}

function parseSessionBootstrapInput(req: Request): SessionBootstrapInput {
  const queryPageUrl = typeof req.query.pageUrl === 'string' ? req.query.pageUrl : null;
  const queryLandingUrl = typeof req.query.landingUrl === 'string' ? req.query.landingUrl : queryPageUrl;
  const referrerUrl = typeof req.query.referrerUrl === 'string' ? req.query.referrerUrl : null;
  const headerReferrerUrl = normalizeNullableUrl(req.header('referer'));
  const pageUrl = normalizeNullableUrl(queryPageUrl) ?? headerReferrerUrl;
  const landingUrl = normalizeNullableUrl(queryLandingUrl) ?? pageUrl;

  return {
    pageUrl,
    landingUrl,
    referrerUrl: normalizeNullableUrl(referrerUrl)
  };
}

function buildRequestContextAttributionCapture(
  req: Request,
  sessionId: string,
  bootstrap: SessionBootstrapInput,
  occurredAt: Date
): RequestContextAttributionCapture | null {
  const ingestionSource =
    typeof req.query.pageUrl === 'string' ||
    typeof req.query.landingUrl === 'string' ||
    typeof req.query.referrerUrl === 'string'
      ? 'request_query'
      : normalizeNullableUrl(req.header('referer'))
        ? 'request_referrer'
        : null;

  const pageUrl = bootstrap.pageUrl ?? bootstrap.landingUrl;
  const landingUrl = bootstrap.landingUrl ?? pageUrl;

  if (!ingestionSource || !pageUrl || !landingUrl) {
    return null;
  }

  const campaignParameters = parseCampaignParameters(landingUrl);

  return {
    ingestionSource,
    consentState: 'unknown',
    input: normalizeAttributionCaptureV1({
      schema_version: ATTRIBUTION_SCHEMA_VERSION,
      roas_radar_session_id: sessionId,
      occurred_at: occurredAt.toISOString(),
      captured_at: occurredAt.toISOString(),
      landing_url: landingUrl,
      referrer_url: bootstrap.referrerUrl,
      page_url: pageUrl,
      utm_source: campaignParameters.utm_source,
      utm_medium: campaignParameters.utm_medium,
      utm_campaign: campaignParameters.utm_campaign,
      utm_content: campaignParameters.utm_content,
      utm_term: campaignParameters.utm_term,
      gclid: campaignParameters.gclid,
      gbraid: campaignParameters.gbraid,
      wbraid: campaignParameters.wbraid,
      fbclid: campaignParameters.fbclid,
      ttclid: campaignParameters.ttclid,
      msclkid: campaignParameters.msclkid
    })
  };
}

async function findSessionIdentity(sessionId: string, client: PoolClient): Promise<SessionIdentityRow | null> {
  const result = await client.query<SessionIdentityRow>(
    `
      SELECT
        roas_radar_session_id,
        created_at
      FROM session_attribution_identities
      WHERE roas_radar_session_id = $1
      LIMIT 1
    `,
    [sessionId]
  );

  return result.rows[0] ?? null;
}

async function persistBootstrapTrackingSession(
  sessionId: string,
  bootstrap: SessionBootstrapInput,
  occurredAt: Date,
  userAgent: string | null,
  ipHash: string | null,
  client: PoolClient
): Promise<void> {
  const sourceUrl = bootstrap.landingUrl ?? bootstrap.pageUrl;

  if (!sourceUrl) {
    await client.query(
      `
        INSERT INTO tracking_sessions (
          id,
          created_at,
          updated_at,
          first_seen_at,
          last_seen_at,
          user_agent,
          ip_hash
        )
        VALUES ($1, now(), now(), $2, $2, $3, $4)
        ON CONFLICT (id)
        DO UPDATE SET
          updated_at = now(),
          last_seen_at = GREATEST(tracking_sessions.last_seen_at, EXCLUDED.last_seen_at),
          user_agent = COALESCE(tracking_sessions.user_agent, EXCLUDED.user_agent),
          ip_hash = COALESCE(tracking_sessions.ip_hash, EXCLUDED.ip_hash)
      `,
      [sessionId, occurredAt, userAgent, ipHash]
    );

    return;
  }

  const params = parseCampaignParameters(sourceUrl);
  const canonicalDimensions = buildCanonicalTouchpointDimensions({
    source: params.utm_source,
    medium: params.utm_medium,
    campaign: params.utm_campaign,
    content: params.utm_content,
    term: params.utm_term,
    gclid: params.gclid,
    gbraid: params.gbraid,
    wbraid: params.wbraid,
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
        initial_gbraid,
        initial_wbraid,
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
        $15,
        $16,
        $17
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
        initial_gbraid = COALESCE(tracking_sessions.initial_gbraid, EXCLUDED.initial_gbraid),
        initial_wbraid = COALESCE(tracking_sessions.initial_wbraid, EXCLUDED.initial_wbraid),
        initial_fbclid = COALESCE(tracking_sessions.initial_fbclid, EXCLUDED.initial_fbclid),
        initial_ttclid = COALESCE(tracking_sessions.initial_ttclid, EXCLUDED.initial_ttclid),
        initial_msclkid = COALESCE(tracking_sessions.initial_msclkid, EXCLUDED.initial_msclkid),
        user_agent = COALESCE(tracking_sessions.user_agent, EXCLUDED.user_agent),
        ip_hash = COALESCE(tracking_sessions.ip_hash, EXCLUDED.ip_hash)
    `,
    [
      sessionId,
      occurredAt,
      sourceUrl,
      bootstrap.referrerUrl ?? null,
      canonicalDimensions.source,
      canonicalDimensions.medium,
      canonicalDimensions.campaign,
      canonicalDimensions.content,
      canonicalDimensions.term,
      params.gclid,
      params.gbraid,
      params.wbraid,
      params.fbclid,
      params.ttclid,
      params.msclkid,
      userAgent,
      ipHash
    ]
  );
}

async function persistSessionBootstrap(
  req: Request,
  sessionId: string,
  bootstrap: SessionBootstrapInput
): Promise<{
  sessionId: string;
  createdAt: string;
  isNewSession: boolean;
  requestContextCaptured: boolean;
  requestContextSource: (typeof REQUEST_CONTEXT_INGESTION_SOURCES)[number] | null;
}> {
  const requestIp = resolveRequestIp(req);
  const userAgent = normalizeNullableString(req.header('user-agent'));
  const ipHash = hashIp(requestIp);
  const now = new Date();
  const requestContextCapture = buildRequestContextAttributionCapture(req, sessionId, bootstrap, now);

  return withTransaction(async (client) => {
    const existing = await findSessionIdentity(sessionId, client);

    await persistBootstrapTrackingSession(sessionId, bootstrap, now, userAgent, ipHash, client);

    const sourceUrl = bootstrap.landingUrl ?? bootstrap.pageUrl;
    const params = sourceUrl ? parseCampaignParameters(sourceUrl) : null;

    await client.query(
      `
        INSERT INTO session_attribution_identities (
          roas_radar_session_id,
          created_at,
          updated_at,
          first_captured_at,
          last_captured_at,
          landing_url,
          referrer_url,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign,
          initial_utm_content,
          initial_utm_term,
          initial_gclid,
          initial_gbraid,
          initial_wbraid,
          initial_fbclid,
          initial_ttclid,
          initial_msclkid
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
        ON CONFLICT (roas_radar_session_id)
        DO UPDATE SET
          updated_at = now(),
          last_captured_at = GREATEST(
            session_attribution_identities.last_captured_at,
            EXCLUDED.last_captured_at
          ),
          landing_url = COALESCE(session_attribution_identities.landing_url, EXCLUDED.landing_url),
          referrer_url = COALESCE(session_attribution_identities.referrer_url, EXCLUDED.referrer_url),
          initial_utm_source = COALESCE(session_attribution_identities.initial_utm_source, EXCLUDED.initial_utm_source),
          initial_utm_medium = COALESCE(session_attribution_identities.initial_utm_medium, EXCLUDED.initial_utm_medium),
          initial_utm_campaign = COALESCE(session_attribution_identities.initial_utm_campaign, EXCLUDED.initial_utm_campaign),
          initial_utm_content = COALESCE(session_attribution_identities.initial_utm_content, EXCLUDED.initial_utm_content),
          initial_utm_term = COALESCE(session_attribution_identities.initial_utm_term, EXCLUDED.initial_utm_term),
          initial_gclid = COALESCE(session_attribution_identities.initial_gclid, EXCLUDED.initial_gclid),
          initial_gbraid = COALESCE(session_attribution_identities.initial_gbraid, EXCLUDED.initial_gbraid),
          initial_wbraid = COALESCE(session_attribution_identities.initial_wbraid, EXCLUDED.initial_wbraid),
          initial_fbclid = COALESCE(session_attribution_identities.initial_fbclid, EXCLUDED.initial_fbclid),
          initial_ttclid = COALESCE(session_attribution_identities.initial_ttclid, EXCLUDED.initial_ttclid),
          initial_msclkid = COALESCE(session_attribution_identities.initial_msclkid, EXCLUDED.initial_msclkid)
      `,
      [
        sessionId,
        now,
        sourceUrl,
        bootstrap.referrerUrl ?? null,
        params?.utm_source ?? null,
        params?.utm_medium ?? null,
        params?.utm_campaign ?? null,
        params?.utm_content ?? null,
        params?.utm_term ?? null,
        params?.gclid ?? null,
        params?.gbraid ?? null,
        params?.wbraid ?? null,
        params?.fbclid ?? null,
        params?.ttclid ?? null,
        params?.msclkid ?? null
      ]
    );

    let requestContextCaptured = false;

    if (requestContextCapture) {
      const fallbackResult = await ingestAttributionCaptureWithClient(client, requestContextCapture.input, {
        consentState: requestContextCapture.consentState,
        eventType: 'page_view',
        ingestionSource: requestContextCapture.ingestionSource,
        deduplicationKey: hashRequestContextCaptureFingerprint(
          requestContextCapture.input,
          requestContextCapture.ingestionSource
        ),
        rawPayload: {
          ...requestContextCapture.input,
          consent_state: requestContextCapture.consentState,
          ingestion_source: requestContextCapture.ingestionSource,
          capture_origin: 'request_context'
        }
      });

      requestContextCaptured = !fallbackResult.deduplicated;
    }

    const metricDate = formatDateInTimezone(now, await getReportingTimezone(client));
    await refreshDailyReportingMetrics(client, [metricDate]);

    return {
      sessionId,
      createdAt: (existing?.created_at ?? now).toISOString(),
      isNewSession: !existing,
      requestContextCaptured,
      requestContextSource: requestContextCapture?.ingestionSource ?? null
    };
  });
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

async function findMatchingFallbackTrackingEvent(input: TrackingEventInput): Promise<ExistingTrackingEventRow | null> {
  if (input.eventType !== 'page_view') {
    return null;
  }

  const result = await query<ExistingTrackingEventRow>(
    `
      SELECT
        id,
        occurred_at,
        ingested_at,
        session_id
      FROM tracking_events
      WHERE session_id = $1::uuid
        AND event_type = $2
        AND page_url = $3
        AND COALESCE(referrer_url, '') = COALESCE($4, '')
        AND ingestion_source = ANY($5::text[])
        AND occurred_at BETWEEN
          $6::timestamptz - ($7::int * interval '1 minute')
          AND $6::timestamptz + ($7::int * interval '1 minute')
      ORDER BY ABS(EXTRACT(EPOCH FROM (occurred_at - $6::timestamptz))) ASC, occurred_at ASC
      LIMIT 1
    `,
    [
      input.sessionId,
      input.eventType,
      input.pageUrl,
      input.referrerUrl ?? null,
      [...REQUEST_CONTEXT_INGESTION_SOURCES],
      new Date(input.occurredAt),
      FALLBACK_BROWSER_DEDUPLICATION_WINDOW_MINUTES
    ]
  );

  return result.rows[0] ?? null;
}

async function findExistingSessionAttributionTouchEventByFingerprint(
  ingestionFingerprint: string
): Promise<ExistingSessionAttributionTouchEventRow | null> {
  const result = await query<ExistingSessionAttributionTouchEventRow>(
    `
      SELECT
        id,
        captured_at,
        roas_radar_session_id
      FROM session_attribution_touch_events
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
    gbraid: params.gbraid,
    wbraid: params.wbraid,
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
        initial_gbraid,
        initial_wbraid,
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
        $15,
        $16,
        $17
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
        initial_gbraid = COALESCE(tracking_sessions.initial_gbraid, EXCLUDED.initial_gbraid),
        initial_wbraid = COALESCE(tracking_sessions.initial_wbraid, EXCLUDED.initial_wbraid),
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
      params.gclid,
      params.gbraid,
      params.wbraid,
      params.fbclid,
      params.ttclid,
      params.msclkid,
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
    gbraid: params.gbraid,
    wbraid: params.wbraid,
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
          gbraid,
          wbraid,
          fbclid,
          ttclid,
          msclkid,
          shopify_cart_token,
          shopify_checkout_token,
          client_event_id,
          consent_state,
          ingestion_fingerprint,
          ingestion_source,
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
          $20,
          $21,
          $22,
          $23,
          now(),
          $24::jsonb
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
        params.gclid,
        params.gbraid,
        params.wbraid,
        params.fbclid,
        params.ttclid,
        params.msclkid,
        normalizeNullableString(input.shopifyCartToken),
        normalizeNullableString(input.shopifyCheckoutToken),
        input.clientEventId ?? null,
        input.consentState,
        ingestionFingerprint,
        'browser',
        JSON.stringify({
          ...input,
          ingestion_source: 'browser',
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

function hashAttributionCaptureFingerprint(input: AttributionCaptureV1, eventType: string): string {
  const fingerprintSource = buildUnifiedEventFingerprintSource({
    sessionId: input.roas_radar_session_id,
    eventType,
    occurredAt: input.occurred_at,
    pageUrl: input.page_url,
    referrerUrl: input.referrer_url,
    utmSource: input.utm_source,
    utmMedium: input.utm_medium,
    utmCampaign: input.utm_campaign,
    utmContent: input.utm_content,
    utmTerm: input.utm_term,
    gclid: input.gclid,
    gbraid: input.gbraid,
    wbraid: input.wbraid,
    fbclid: input.fbclid,
    ttclid: input.ttclid,
    msclkid: input.msclkid
  });

  return createHash('sha256').update(fingerprintSource).digest('hex');
}

function hashRequestContextCaptureFingerprint(
  input: AttributionCaptureV1,
  ingestionSource: (typeof REQUEST_CONTEXT_INGESTION_SOURCES)[number]
): string {
  return createHash('sha256')
    .update(
      JSON.stringify({
        roas_radar_session_id: input.roas_radar_session_id,
        landing_url: input.landing_url,
        referrer_url: input.referrer_url,
        page_url: input.page_url,
        utm_source: input.utm_source,
        utm_medium: input.utm_medium,
        utm_campaign: input.utm_campaign,
        utm_content: input.utm_content,
        utm_term: input.utm_term,
        gclid: input.gclid,
        gbraid: input.gbraid,
        wbraid: input.wbraid,
        fbclid: input.fbclid,
        ttclid: input.ttclid,
        msclkid: input.msclkid,
        ingestion_source: ingestionSource
      })
    )
    .digest('hex');
}

function buildCanonicalDimensionsFromCapture(input: AttributionCaptureV1) {
  return buildCanonicalTouchpointDimensions({
    source: input.utm_source,
    medium: input.utm_medium,
    campaign: input.utm_campaign,
    content: input.utm_content,
    term: input.utm_term,
    gclid: input.gclid,
    gbraid: input.gbraid,
    wbraid: input.wbraid,
    fbclid: input.fbclid,
    ttclid: input.ttclid,
    msclkid: input.msclkid
  });
}

async function upsertTrackingSessionFromAttributionCapture(
  input: AttributionCaptureV1,
  client: PoolClient
): Promise<void> {
  const canonicalDimensions = buildCanonicalDimensionsFromCapture(input);
  const occurredAt = new Date(input.occurred_at);

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
        initial_gbraid,
        initial_wbraid,
        initial_fbclid,
        initial_ttclid,
        initial_msclkid
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
        initial_gbraid = COALESCE(tracking_sessions.initial_gbraid, EXCLUDED.initial_gbraid),
        initial_wbraid = COALESCE(tracking_sessions.initial_wbraid, EXCLUDED.initial_wbraid),
        initial_fbclid = COALESCE(tracking_sessions.initial_fbclid, EXCLUDED.initial_fbclid),
        initial_ttclid = COALESCE(tracking_sessions.initial_ttclid, EXCLUDED.initial_ttclid),
        initial_msclkid = COALESCE(tracking_sessions.initial_msclkid, EXCLUDED.initial_msclkid)
    `,
    [
      input.roas_radar_session_id,
      occurredAt,
      input.landing_url,
      input.referrer_url,
      canonicalDimensions.source,
      canonicalDimensions.medium,
      canonicalDimensions.campaign,
      canonicalDimensions.content,
      canonicalDimensions.term,
      input.gclid,
      input.gbraid,
      input.wbraid,
      input.fbclid,
      input.ttclid,
      input.msclkid
    ]
  );
}

async function upsertSessionAttributionIdentityFromCapture(
  input: AttributionCaptureV1,
  client: PoolClient
): Promise<void> {
  const canonicalDimensions = buildCanonicalDimensionsFromCapture(input);
  const capturedAt = new Date(input.captured_at);

  await client.query(
    `
      INSERT INTO session_attribution_identities (
        roas_radar_session_id,
        created_at,
        updated_at,
        first_captured_at,
        last_captured_at,
        landing_url,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_gbraid,
        initial_wbraid,
        initial_fbclid,
        initial_ttclid,
        initial_msclkid
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
      ON CONFLICT (roas_radar_session_id)
      DO UPDATE SET
        updated_at = now(),
        last_captured_at = GREATEST(
          session_attribution_identities.last_captured_at,
          EXCLUDED.last_captured_at
        ),
        landing_url = COALESCE(session_attribution_identities.landing_url, EXCLUDED.landing_url),
        referrer_url = COALESCE(session_attribution_identities.referrer_url, EXCLUDED.referrer_url),
        initial_utm_source = COALESCE(session_attribution_identities.initial_utm_source, EXCLUDED.initial_utm_source),
        initial_utm_medium = COALESCE(session_attribution_identities.initial_utm_medium, EXCLUDED.initial_utm_medium),
        initial_utm_campaign = COALESCE(session_attribution_identities.initial_utm_campaign, EXCLUDED.initial_utm_campaign),
        initial_utm_content = COALESCE(session_attribution_identities.initial_utm_content, EXCLUDED.initial_utm_content),
        initial_utm_term = COALESCE(session_attribution_identities.initial_utm_term, EXCLUDED.initial_utm_term),
        initial_gclid = COALESCE(session_attribution_identities.initial_gclid, EXCLUDED.initial_gclid),
        initial_gbraid = COALESCE(session_attribution_identities.initial_gbraid, EXCLUDED.initial_gbraid),
        initial_wbraid = COALESCE(session_attribution_identities.initial_wbraid, EXCLUDED.initial_wbraid),
        initial_fbclid = COALESCE(session_attribution_identities.initial_fbclid, EXCLUDED.initial_fbclid),
        initial_ttclid = COALESCE(session_attribution_identities.initial_ttclid, EXCLUDED.initial_ttclid),
        initial_msclkid = COALESCE(session_attribution_identities.initial_msclkid, EXCLUDED.initial_msclkid)
    `,
    [
      input.roas_radar_session_id,
      capturedAt,
      input.landing_url,
      input.referrer_url,
      canonicalDimensions.source,
      canonicalDimensions.medium,
      canonicalDimensions.campaign,
      canonicalDimensions.content,
      canonicalDimensions.term,
      input.gclid,
      input.gbraid,
      input.wbraid,
      input.fbclid,
      input.ttclid,
      input.msclkid
    ]
  );
}

async function insertSessionAttributionTouchEvent(
  client: PoolClient,
  input: AttributionCaptureV1,
  ingestionFingerprint: string,
  options: AttributionCaptureInsertOptions
): Promise<{
  touchEvent: ExistingSessionAttributionTouchEventRow;
  deduplicated: boolean;
}> {
  const canonicalDimensions = buildCanonicalDimensionsFromCapture(input);

  try {
    const result = await client.query<ExistingSessionAttributionTouchEventRow>(
      `
        INSERT INTO session_attribution_touch_events (
          roas_radar_session_id,
          event_type,
          occurred_at,
          captured_at,
          page_url,
          referrer_url,
          utm_source,
          utm_medium,
          utm_campaign,
          utm_content,
          utm_term,
          gclid,
          gbraid,
          wbraid,
          fbclid,
          ttclid,
          msclkid,
          consent_state,
          ingestion_source,
          ingestion_fingerprint,
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
          $20,
          $21::jsonb
        )
        RETURNING id, captured_at, roas_radar_session_id
      `,
      [
        input.roas_radar_session_id,
        options.eventType,
        new Date(input.occurred_at),
        new Date(input.captured_at),
        input.page_url,
        input.referrer_url,
        canonicalDimensions.source,
        canonicalDimensions.medium,
        canonicalDimensions.campaign,
        canonicalDimensions.content,
        canonicalDimensions.term,
        input.gclid,
        input.gbraid,
        input.wbraid,
        input.fbclid,
        input.ttclid,
        input.msclkid,
        options.consentState,
        options.ingestionSource,
        ingestionFingerprint,
        JSON.stringify(options.rawPayload)
      ]
    );

    return {
      touchEvent: result.rows[0],
      deduplicated: false
    };
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
      const existing = await findExistingSessionAttributionTouchEventByFingerprint(ingestionFingerprint);

      if (existing) {
        return {
          touchEvent: existing,
          deduplicated: true
        };
      }
    }

    throw error;
  }
}

async function insertTrackingEventFromAttributionCapture(
  client: PoolClient,
  input: AttributionCaptureV1,
  ingestionFingerprint: string,
  options: AttributionCaptureInsertOptions
): Promise<string> {
  const canonicalDimensions = buildCanonicalDimensionsFromCapture(input);
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
          gbraid,
          wbraid,
          fbclid,
          ttclid,
          msclkid,
          consent_state,
          ingestion_fingerprint,
          ingestion_source,
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
          $20,
          now(),
          $21::jsonb
        )
      `,
      [
        eventId,
        input.roas_radar_session_id,
        options.eventType,
        new Date(input.occurred_at),
        input.page_url,
        input.referrer_url,
        canonicalDimensions.source,
        canonicalDimensions.medium,
        canonicalDimensions.campaign,
        canonicalDimensions.content,
        canonicalDimensions.term,
        input.gclid,
        input.gbraid,
        input.wbraid,
        input.fbclid,
        input.ttclid,
        input.msclkid,
        options.consentState,
        ingestionFingerprint,
        options.ingestionSource,
        JSON.stringify(options.rawPayload)
      ]
    );
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
      const existing = await findExistingTrackingEventByFingerprint(ingestionFingerprint);

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

function logTrackingPipelineOutcome(
  leg: 'browser' | 'server_attribution',
  outcome: 'accepted' | 'deduplicated' | 'failed' | 'rejected',
  fields: Record<string, unknown>
): void {
  const payload = {
    pipeline: 'dual_write',
    leg,
    outcome,
    ...fields
  };

  if (outcome === 'failed' || outcome === 'rejected') {
    logWarning('tracking_pipeline_leg_outcome', payload);
    return;
  }

  logInfo('tracking_pipeline_leg_outcome', payload);
}

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

function parseInput<TSchema extends z.ZodTypeAny>(schema: TSchema, input: unknown): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new TrackingHttpError(400, 'invalid_request', 'Invalid tracking request', error.flatten());
    }

    throw error;
  }
}

function parseAttributionCaptureRequestBody(
  body: TrackingRequestBody
): {
  capture: AttributionCaptureV1;
  consentState: AttributionConsentState;
} {
  const payload = parseTrackingRequestBody(body);

  try {
    return {
      capture: normalizeAttributionCaptureV1(payload),
      consentState: normalizeAttributionConsentState(
        typeof payload.consent_state === 'string' ? payload.consent_state : undefined
      )
    };
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new TrackingHttpError(400, 'invalid_request', 'Invalid attribution capture payload', error.flatten());
    }

    throw error;
  }
}

function enforceRateLimit(req: Request): void {
  const requestIp = resolveRequestIp(req);
  const sessionHint =
    typeof req.body?.sessionId === 'string'
      ? req.body.sessionId
      : typeof req.body?.roas_radar_session_id === 'string'
        ? req.body.roas_radar_session_id
        : 'anonymous';
  const key = createHash('sha256').update(`${requestIp ?? 'unknown'}:${sessionHint}`).digest('hex');
  const result = trackingRateLimiter.check(key);

  if (!result.allowed) {
    throw new TrackingHttpError(429, 'rate_limit_exceeded', 'Too many tracking requests', {
      retryAfterMs: Math.max(result.resetAt - Date.now(), 0)
    });
  }
}

async function ingestTrackingEventBrowserLeg(
  input: TrackingEventInput,
  requestIp: string | undefined
): Promise<BrowserTrackingIngestResult> {
  const sanitizedInput = sanitizeTrackingInput(input);
  const ipHash = hashIp(requestIp);
  const deduplicationKey = hashTrackingFingerprint(sanitizedInput);

  if (sanitizedInput.clientEventId) {
    const existing = await findExistingTrackingEventByClientEventId(sanitizedInput.clientEventId);

    if (existing) {
      return {
        eventId: existing.id,
        ingestedAt: existing.ingested_at.toISOString(),
        sessionId: existing.session_id,
        deduplicated: true,
        sanitizedInput,
        deduplicationKey
      };
    }
  }

  const matchingFallbackEvent = await findMatchingFallbackTrackingEvent(sanitizedInput);

  if (matchingFallbackEvent) {
    return {
      eventId: matchingFallbackEvent.id,
      ingestedAt: matchingFallbackEvent.ingested_at.toISOString(),
      sessionId: matchingFallbackEvent.session_id,
      deduplicated: true,
      sanitizedInput,
      deduplicationKey
    };
  }

  const duplicatePayload = await findExistingTrackingEventByFingerprint(deduplicationKey);

  if (duplicatePayload) {
    return {
      eventId: duplicatePayload.id,
      ingestedAt: duplicatePayload.ingested_at.toISOString(),
      sessionId: duplicatePayload.session_id,
      deduplicated: true,
      sanitizedInput,
      deduplicationKey
    };
  }

  const eventId = await withTransaction(async (client) => {
    const occurredAt = new Date(sanitizedInput.occurredAt);
    const userAgent = sanitizedInput.context.userAgent ?? null;
    const metricDate = formatDateInTimezone(occurredAt, await getReportingTimezone(client));

    await upsertTrackingSession(sanitizedInput, occurredAt, userAgent, ipHash, client);
    const insertedEventId = await insertTrackingEvent(client, sanitizedInput, deduplicationKey);
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
    deduplicated: false,
    sanitizedInput,
    deduplicationKey
  };
}

async function emitServerAttributionFromTrackingEvent(input: TrackingEventInput): Promise<ServerAttributionResult> {
  const attributionCapture = buildTrackingAttributionCapture(input, new Date().toISOString());

  try {
    const result = await withTransaction(async (client) => {
      await upsertSessionAttributionIdentityFromCapture(attributionCapture, client);
      await upsertTrackingSessionFromAttributionCapture(attributionCapture, client);

      return ingestAttributionCaptureWithClient(client, attributionCapture, {
        consentState: input.consentState,
        eventType: input.eventType,
        ingestionSource: 'server',
        deduplicationKey: hashAttributionCaptureFingerprint(attributionCapture, input.eventType),
        mirrorTrackingEvent: false,
        rawPayload: {
          ...attributionCapture,
          consent_state: input.consentState,
          event_type: input.eventType,
          client_event_id: input.clientEventId,
          ingestion_source: 'server',
          capture_origin: 'browser_dual_write'
        }
      });
    });

    return {
      ok: true,
      touchEventId: result.touchEvent.id,
      trackingEventId: result.trackingEventId,
      capturedAt: result.touchEvent.captured_at.toISOString(),
      deduplicated: result.deduplicated,
      errorCode: null
    };
  } catch (error) {
    logError('tracking_server_attribution_emit_failed', error, {
      sessionId: input.sessionId,
      eventType: input.eventType
    });

    return {
      ok: false,
      touchEventId: null,
      trackingEventId: null,
      capturedAt: null,
      deduplicated: false,
      errorCode: 'server_attribution_emit_failed'
    };
  }
}

async function ingestAttributionCaptureWithClient(
  client: PoolClient,
  input: AttributionCaptureV1,
  options: AttributionCaptureInsertOptions
): Promise<{
  touchEvent: ExistingSessionAttributionTouchEventRow;
  deduplicated: boolean;
  trackingEventId: string | null;
}> {
  const touchEventInsert = await insertSessionAttributionTouchEvent(client, input, options.deduplicationKey, options);
  let trackingEventId: string | null = null;

  if (!touchEventInsert.deduplicated && options.mirrorTrackingEvent !== false) {
    trackingEventId = await insertTrackingEventFromAttributionCapture(client, input, options.deduplicationKey, options);
  }

  return {
    touchEvent: touchEventInsert.touchEvent,
    deduplicated: touchEventInsert.deduplicated,
    trackingEventId
  };
}

async function ingestAttributionCapture(
  input: AttributionCaptureV1,
  consentState: AttributionConsentState
): Promise<{
  touchEventId: number;
  trackingEventId: string | null;
  capturedAt: string;
  sessionId: string;
  deduplicated: boolean;
}> {
  const deduplicationKey = hashAttributionCaptureFingerprint(input, 'page_view');
  const existingTouchEvent = await findExistingSessionAttributionTouchEventByFingerprint(deduplicationKey);

  if (existingTouchEvent) {
    return {
      touchEventId: existingTouchEvent.id,
      trackingEventId: null,
      capturedAt: existingTouchEvent.captured_at.toISOString(),
      sessionId: existingTouchEvent.roas_radar_session_id,
      deduplicated: true
    };
  }

  const result = await withTransaction(async (client) => {
    await upsertSessionAttributionIdentityFromCapture(input, client);
    await upsertTrackingSessionFromAttributionCapture(input, client);
    return ingestAttributionCaptureWithClient(client, input, {
      consentState,
      eventType: 'page_view',
      ingestionSource: 'server',
      deduplicationKey,
      rawPayload: {
        ...input,
        schema_version: ATTRIBUTION_SCHEMA_VERSION,
        consent_state: consentState,
        ingestion_source: 'server'
      }
    });
  });

  return {
    touchEventId: result.touchEvent.id,
    trackingEventId: result.trackingEventId,
    capturedAt: result.touchEvent.captured_at.toISOString(),
    sessionId: result.touchEvent.roas_radar_session_id,
    deduplicated: result.deduplicated
  };
}

export function createTrackingRouter(): Router {
  const router = Router();

  router.get('/session', async (req, res, next) => {
    try {
      enforceAllowedOrigin(req);

      const bootstrapInput = sessionBootstrapSchema.parse(parseSessionBootstrapInput(req));
      const sessionId = resolveSessionIdFromRequest(req) ?? randomUUID();
      const result = await persistSessionBootstrap(req, sessionId, bootstrapInput);

      appendTrackingSessionCookies(req, res, result.sessionId);
      logInfo(result.isNewSession ? 'tracking_session_bootstrap_created' : 'tracking_session_bootstrap_reused', {
        sessionId: result.sessionId,
        isNewSession: result.isNewSession,
        requestContextCaptured: result.requestContextCaptured,
        requestContextSource: result.requestContextSource
      });

      res.status(200).json({
        ok: true,
        sessionId: result.sessionId,
        createdAt: result.createdAt,
        isNewSession: result.isNewSession
      });
    } catch (error) {
      if (error instanceof TrackingHttpError) {
        logWarning('tracking_session_bootstrap_rejected', {
          code: error.code,
          statusCode: error.statusCode,
          details: error.details
        });
      } else {
        logError('tracking_session_bootstrap_failed', error, {
          path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path
        });
      }

      next(error);
    }
  });

  router.post('/attribution', async (req, res, next) => {
    try {
      enforceSupportedContentType(req);
      enforceAllowedOrigin(req);
      enforceRateLimit(req);

      const parsed = parseAttributionCaptureRequestBody(req.body as TrackingRequestBody);
      const result = await ingestAttributionCapture(parsed.capture, parsed.consentState);

      logInfo(result.deduplicated ? 'tracking_attribution_ingest_duplicate' : 'tracking_attribution_ingest_accepted', {
        sessionId: result.sessionId,
        touchEventId: result.touchEventId,
        deduplicated: result.deduplicated
      });

      res.status(200).json({
        ok: true,
        sessionId: result.sessionId,
        touchEventId: result.touchEventId,
        trackingEventId: result.trackingEventId,
        capturedAt: result.capturedAt,
        deduplicated: result.deduplicated
      });
    } catch (error) {
      if (error instanceof TrackingHttpError) {
        logWarning('tracking_attribution_ingest_rejected', {
          code: error.code,
          statusCode: error.statusCode,
          details: error.details
        });
      } else {
        logError('tracking_attribution_ingest_failed', error, {
          path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path
        });
      }

      next(error);
    }
  });

  router.post('/', async (req, res, next) => {
    try {
      enforceSupportedContentType(req);
      enforceAllowedOrigin(req);
      enforceRateLimit(req);

      const body = parseTrackingRequestBody(req.body as TrackingRequestBody);
      const input = parseInput(trackingEventSchema, body);
      const browserResult = await ingestTrackingEventBrowserLeg(input, resolveRequestIp(req));
      const serverAttributionResult = await emitServerAttributionFromTrackingEvent(browserResult.sanitizedInput);

      logInfo(browserResult.deduplicated ? 'tracking_ingest_duplicate' : 'tracking_ingest_accepted', {
        sessionId: browserResult.sessionId,
        eventId: browserResult.eventId,
        eventType: input.eventType,
        deduplicated: browserResult.deduplicated
      });
      logTrackingPipelineOutcome('browser', browserResult.deduplicated ? 'deduplicated' : 'accepted', {
        sessionId: browserResult.sessionId,
        eventId: browserResult.eventId,
        eventType: input.eventType
      });
      logTrackingPipelineOutcome(
        'server_attribution',
        serverAttributionResult.ok
          ? serverAttributionResult.deduplicated
            ? 'deduplicated'
            : 'accepted'
          : 'failed',
        {
          sessionId: browserResult.sessionId,
          eventId: browserResult.eventId,
          eventType: input.eventType,
          touchEventId: serverAttributionResult.touchEventId,
          errorCode: serverAttributionResult.errorCode
        }
      );

      appendTrackingSessionCookies(req, res, browserResult.sessionId);
      res.status(200).json({
        ok: true,
        eventId: browserResult.eventId,
        ingestedAt: browserResult.ingestedAt,
        sessionId: browserResult.sessionId,
        deduplicated: browserResult.deduplicated,
        attribution: serverAttributionResult
      });
    } catch (error) {
      if (error instanceof TrackingHttpError) {
        logWarning('tracking_ingest_rejected', {
          code: error.code,
          statusCode: error.statusCode,
          details: error.details
        });
        logTrackingPipelineOutcome('browser', 'rejected', {
          code: error.code,
          statusCode: error.statusCode
        });
        logTrackingPipelineOutcome('server_attribution', 'rejected', {
          code: error.code,
          statusCode: error.statusCode
        });
      } else {
        logError('tracking_ingest_failed', error, {
          path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path
        });
        logTrackingPipelineOutcome('browser', 'failed', {
          path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path
        });
        logTrackingPipelineOutcome('server_attribution', 'failed', {
          path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path
        });
      }
      next(error);
    }
  });

  return router;
}
