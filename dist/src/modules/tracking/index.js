import { createHash, randomUUID } from 'node:crypto';
import { Router } from 'express';
import { z } from 'zod';
import { ATTRIBUTION_SCHEMA_VERSION, MAX_ATTRIBUTION_URL_LENGTH, attributionConsentStateSchema, normalizeAttributionCaptureV1, normalizeAttributionConsentState, normalizeAttributionString, normalizeAttributionUrl } from '../../../packages/attribution-schema/index.js';
import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo, logWarning, summarizeAttributionObservation, summarizeDualWriteConsistency } from '../../observability/index.js';
import { enqueueAttributionForTrackingTouchpoint } from '../attribution/index.js';
import { buildCanonicalTouchpointDimensions } from '../marketing-dimensions/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { formatDateInTimezone, getReportingTimezone } from '../settings/index.js';
const EVENT_TYPES = ['page_view', 'product_view', 'add_to_cart', 'checkout_started'];
const MAX_TOKEN_LENGTH = 255;
const MAX_USER_AGENT_LENGTH = 1024;
const MAX_LANGUAGE_LENGTH = 64;
const MAX_SCREEN_LENGTH = 64;
const MAX_CLIENT_EVENT_ID_LENGTH = 128;
class TrackingHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'TrackingHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
const trimmedStringSchema = (maxLength) => z
    .string()
    .trim()
    .min(1)
    .max(maxLength);
const nullableTrimmedStringSchema = (maxLength) => z
    .union([z.string(), z.null(), z.undefined()])
    .transform((value) => normalizeAttributionString(value))
    .refine((value) => value === null || value.length <= maxLength, {
    message: `String must contain at most ${maxLength} character(s)`
});
const trackingEventSchema = z
    .object({
    eventType: z.enum(EVENT_TYPES),
    occurredAt: z.string().datetime({ offset: true }),
    sessionId: z.string().uuid(),
    pageUrl: trimmedStringSchema(MAX_ATTRIBUTION_URL_LENGTH),
    referrerUrl: nullableTrimmedStringSchema(MAX_ATTRIBUTION_URL_LENGTH),
    shopifyCartToken: nullableTrimmedStringSchema(MAX_TOKEN_LENGTH),
    shopifyCheckoutToken: nullableTrimmedStringSchema(MAX_TOKEN_LENGTH),
    clientEventId: nullableTrimmedStringSchema(MAX_CLIENT_EVENT_ID_LENGTH),
    consentState: attributionConsentStateSchema.optional(),
    context: z
        .object({
        userAgent: nullableTrimmedStringSchema(MAX_USER_AGENT_LENGTH),
        screen: nullableTrimmedStringSchema(MAX_SCREEN_LENGTH),
        language: nullableTrimmedStringSchema(MAX_LANGUAGE_LENGTH)
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
const attributionCaptureRequestSchema = z
    .object({
    schema_version: z.literal(ATTRIBUTION_SCHEMA_VERSION),
    roas_radar_session_id: z.string(),
    occurred_at: z.string(),
    captured_at: z.string(),
    landing_url: z.union([z.string(), z.null(), z.undefined()]).optional(),
    referrer_url: z.union([z.string(), z.null(), z.undefined()]).optional(),
    page_url: z.union([z.string(), z.null(), z.undefined()]).optional(),
    utm_source: z.union([z.string(), z.null(), z.undefined()]).optional(),
    utm_medium: z.union([z.string(), z.null(), z.undefined()]).optional(),
    utm_campaign: z.union([z.string(), z.null(), z.undefined()]).optional(),
    utm_content: z.union([z.string(), z.null(), z.undefined()]).optional(),
    utm_term: z.union([z.string(), z.null(), z.undefined()]).optional(),
    gclid: z.union([z.string(), z.null(), z.undefined()]).optional(),
    gbraid: z.union([z.string(), z.null(), z.undefined()]).optional(),
    wbraid: z.union([z.string(), z.null(), z.undefined()]).optional(),
    fbclid: z.union([z.string(), z.null(), z.undefined()]).optional(),
    ttclid: z.union([z.string(), z.null(), z.undefined()]).optional(),
    msclkid: z.union([z.string(), z.null(), z.undefined()]).optional(),
    consent_state: attributionConsentStateSchema.optional()
})
    .transform((value) => ({
    capture: normalizeAttributionCaptureV1(value),
    consentState: normalizeAttributionConsentState(value.consent_state)
}));
const sessionBootstrapQuerySchema = z.object({
    pageUrl: trimmedStringSchema(MAX_ATTRIBUTION_URL_LENGTH),
    landingUrl: nullableTrimmedStringSchema(MAX_ATTRIBUTION_URL_LENGTH),
    referrerUrl: nullableTrimmedStringSchema(MAX_ATTRIBUTION_URL_LENGTH)
});
function logAttributionCaptureObserved(source, payload, fields) {
    logInfo('attribution_capture_observed', {
        source,
        ...summarizeAttributionObservation(payload),
        ...fields
    });
}
function validateTrackingTimestamp(value, ctx) {
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
function validateHttpUrl(value, field, ctx) {
    try {
        normalizeAttributionUrl(value);
    }
    catch (error) {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: error instanceof Error && error.message === 'invalid_protocol' ? `${field} must use http or https` : `${field} must be a valid URL`,
            path: [field]
        });
    }
}
function normalizeTrackingUrl(rawUrl) {
    try {
        return normalizeAttributionUrl(rawUrl);
    }
    catch {
        return null;
    }
}
function normalizeNullableString(value) {
    return normalizeAttributionString(value);
}
function resolveRequestIp(req) {
    const forwardedFor = req.header('x-forwarded-for');
    if (forwardedFor) {
        return normalizeNullableString(forwardedFor.split(',')[0]);
    }
    return normalizeNullableString(req.ip);
}
function hashIp(value) {
    const normalized = normalizeNullableString(value);
    if (!normalized) {
        return null;
    }
    return createHash('sha256').update(normalized).digest('hex');
}
function hashTrackingFingerprint(input) {
    return createHash('sha256')
        .update(JSON.stringify({
        sessionId: input.sessionId,
        eventType: input.eventType,
        occurredAt: input.occurredAt,
        pageUrl: input.pageUrl,
        referrerUrl: input.referrerUrl,
        shopifyCartToken: input.shopifyCartToken,
        shopifyCheckoutToken: input.shopifyCheckoutToken,
        clientEventId: input.clientEventId
    }))
        .digest('hex');
}
function hashAttributionFingerprint(capture, eventType, ingestionSource) {
    return createHash('sha256')
        .update(JSON.stringify({
        schemaVersion: capture.schema_version,
        sessionId: capture.roas_radar_session_id,
        eventType,
        occurredAt: capture.occurred_at,
        capturedAt: capture.captured_at,
        landingUrl: capture.landing_url,
        referrerUrl: capture.referrer_url,
        pageUrl: capture.page_url,
        utmSource: capture.utm_source,
        utmMedium: capture.utm_medium,
        utmCampaign: capture.utm_campaign,
        utmContent: capture.utm_content,
        utmTerm: capture.utm_term,
        gclid: capture.gclid,
        gbraid: capture.gbraid,
        wbraid: capture.wbraid,
        fbclid: capture.fbclid,
        ttclid: capture.ttclid,
        msclkid: capture.msclkid,
        ingestionSource
    }))
        .digest('hex');
}
function parseCampaignParameters(pageUrl) {
    const url = new URL(pageUrl);
    return {
        utm_source: normalizeNullableString(url.searchParams.get('utm_source'))?.toLowerCase() ?? null,
        utm_medium: normalizeNullableString(url.searchParams.get('utm_medium'))?.toLowerCase() ?? null,
        utm_campaign: normalizeNullableString(url.searchParams.get('utm_campaign'))?.toLowerCase() ?? null,
        utm_content: normalizeNullableString(url.searchParams.get('utm_content'))?.toLowerCase() ?? null,
        utm_term: normalizeNullableString(url.searchParams.get('utm_term'))?.toLowerCase() ?? null,
        gclid: normalizeNullableString(url.searchParams.get('gclid')),
        gbraid: normalizeNullableString(url.searchParams.get('gbraid')),
        wbraid: normalizeNullableString(url.searchParams.get('wbraid')),
        fbclid: normalizeNullableString(url.searchParams.get('fbclid')),
        ttclid: normalizeNullableString(url.searchParams.get('ttclid')),
        msclkid: normalizeNullableString(url.searchParams.get('msclkid'))
    };
}
function buildCaptureFromTrackingEvent(input) {
    const marketingDimensions = parseCampaignParameters(input.pageUrl);
    return normalizeAttributionCaptureV1({
        schema_version: ATTRIBUTION_SCHEMA_VERSION,
        roas_radar_session_id: input.sessionId,
        occurred_at: input.occurredAt,
        captured_at: new Date().toISOString(),
        landing_url: null,
        referrer_url: input.referrerUrl,
        page_url: input.pageUrl,
        ...marketingDimensions
    });
}
function sanitizeTrackingInput(input) {
    return {
        eventType: input.eventType,
        occurredAt: new Date(input.occurredAt).toISOString(),
        sessionId: input.sessionId,
        pageUrl: normalizeTrackingUrl(input.pageUrl) ?? input.pageUrl,
        referrerUrl: normalizeTrackingUrl(input.referrerUrl),
        shopifyCartToken: normalizeNullableString(input.shopifyCartToken),
        shopifyCheckoutToken: normalizeNullableString(input.shopifyCheckoutToken),
        clientEventId: normalizeNullableString(input.clientEventId),
        consentState: normalizeAttributionConsentState(input.consentState),
        context: {
            userAgent: normalizeNullableString(input.context.userAgent),
            screen: normalizeNullableString(input.context.screen),
            language: normalizeNullableString(input.context.language)
        }
    };
}
async function findExistingTrackingEventByClientEventId(clientEventId) {
    const result = await query(`
      SELECT
        id::text AS id,
        occurred_at,
        ingested_at,
        session_id::text AS session_id
      FROM tracking_events
      WHERE client_event_id = $1
      LIMIT 1
    `, [clientEventId]);
    return result.rows[0] ?? null;
}
async function findExistingTrackingEventByFingerprint(ingestionFingerprint) {
    const result = await query(`
      SELECT
        id::text AS id,
        occurred_at,
        ingested_at,
        session_id::text AS session_id
      FROM tracking_events
      WHERE ingestion_fingerprint = $1
      LIMIT 1
    `, [ingestionFingerprint]);
    return result.rows[0] ?? null;
}
async function findExistingAttributionTouchEventByFingerprint(ingestionFingerprint) {
    const result = await query(`
      SELECT
        id,
        captured_at,
        roas_radar_session_id::text AS roas_radar_session_id
      FROM session_attribution_touch_events
      WHERE ingestion_fingerprint = $1
      LIMIT 1
    `, [ingestionFingerprint]);
    return result.rows[0] ?? null;
}
async function upsertTrackingSessionForCapture(client, capture, occurredAt, userAgent, ipHash) {
    const canonicalDimensions = buildCanonicalTouchpointDimensions({
        source: capture.utm_source,
        medium: capture.utm_medium,
        campaign: capture.utm_campaign,
        content: capture.utm_content,
        term: capture.utm_term,
        gclid: capture.gclid,
        gbraid: capture.gbraid,
        wbraid: capture.wbraid,
        fbclid: capture.fbclid,
        ttclid: capture.ttclid,
        msclkid: capture.msclkid
    });
    await client.query(`
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
        $1::uuid,
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
    `, [
        capture.roas_radar_session_id,
        occurredAt,
        capture.landing_url ?? capture.page_url,
        capture.referrer_url,
        canonicalDimensions.source,
        canonicalDimensions.medium,
        canonicalDimensions.campaign,
        canonicalDimensions.content,
        canonicalDimensions.term,
        capture.gclid,
        capture.gbraid,
        capture.wbraid,
        capture.fbclid,
        capture.ttclid,
        capture.msclkid,
        userAgent,
        ipHash
    ]);
}
async function upsertSessionAttributionIdentity(client, capture) {
    await client.query(`
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
        $1::uuid,
        now(),
        now(),
        $2::timestamptz,
        $3::timestamptz,
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
        $16
      )
      ON CONFLICT (roas_radar_session_id)
      DO UPDATE SET
        updated_at = now(),
        first_captured_at = LEAST(session_attribution_identities.first_captured_at, EXCLUDED.first_captured_at),
        last_captured_at = GREATEST(session_attribution_identities.last_captured_at, EXCLUDED.last_captured_at),
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
    `, [
        capture.roas_radar_session_id,
        capture.occurred_at,
        capture.captured_at,
        capture.landing_url,
        capture.referrer_url,
        capture.utm_source,
        capture.utm_medium,
        capture.utm_campaign,
        capture.utm_content,
        capture.utm_term,
        capture.gclid,
        capture.gbraid,
        capture.wbraid,
        capture.fbclid,
        capture.ttclid,
        capture.msclkid
    ]);
}
async function insertTrackingEventForCapture(client, input) {
    const eventId = randomUUID();
    try {
        await client.query(`
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
          raw_payload
        )
        VALUES (
          $1::uuid,
          $2::uuid,
          $3,
          $4::timestamptz,
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
      `, [
            eventId,
            input.capture.roas_radar_session_id,
            input.eventType,
            input.capture.occurred_at,
            input.capture.page_url,
            input.capture.referrer_url,
            input.capture.utm_source,
            input.capture.utm_medium,
            input.capture.utm_campaign,
            input.capture.utm_content,
            input.capture.utm_term,
            input.capture.gclid,
            input.capture.gbraid,
            input.capture.wbraid,
            input.capture.fbclid,
            input.capture.ttclid,
            input.capture.msclkid,
            input.consentState,
            input.ingestionFingerprint,
            input.ingestionSource,
            JSON.stringify(input.rawPayload)
        ]);
    }
    catch (error) {
        if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
            const existing = await findExistingTrackingEventByFingerprint(input.ingestionFingerprint);
            if (existing) {
                return existing.id;
            }
        }
        throw error;
    }
    return eventId;
}
async function insertTrackingBrowserEvent(client, input, rawPayload, ingestionFingerprint) {
    const capture = buildCaptureFromTrackingEvent(input);
    const eventId = randomUUID();
    try {
        await client.query(`
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
          raw_payload
        )
        VALUES (
          $1::uuid,
          $2::uuid,
          $3,
          $4::timestamptz,
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
          $24::jsonb
        )
      `, [
            eventId,
            input.sessionId,
            input.eventType,
            input.occurredAt,
            input.pageUrl,
            input.referrerUrl,
            capture.utm_source,
            capture.utm_medium,
            capture.utm_campaign,
            capture.utm_content,
            capture.utm_term,
            capture.gclid,
            capture.gbraid,
            capture.wbraid,
            capture.fbclid,
            capture.ttclid,
            capture.msclkid,
            input.shopifyCartToken,
            input.shopifyCheckoutToken,
            input.clientEventId,
            input.consentState,
            ingestionFingerprint,
            'browser',
            JSON.stringify(rawPayload)
        ]);
    }
    catch (error) {
        if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
            const existing = (input.clientEventId ? await findExistingTrackingEventByClientEventId(input.clientEventId) : null) ??
                (await findExistingTrackingEventByFingerprint(ingestionFingerprint));
            if (existing) {
                return existing.id;
            }
        }
        throw error;
    }
    return eventId;
}
async function insertAttributionTouchEvent(client, input) {
    try {
        const result = await client.query(`
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
          raw_payload,
          shopify_cart_token,
          shopify_checkout_token
        )
        VALUES (
          $1::uuid,
          $2,
          $3::timestamptz,
          $4::timestamptz,
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
          $21::jsonb,
          $22,
          $23
        )
        RETURNING id
      `, [
            input.capture.roas_radar_session_id,
            input.eventType,
            input.capture.occurred_at,
            input.capture.captured_at,
            input.capture.page_url,
            input.capture.referrer_url,
            input.capture.utm_source,
            input.capture.utm_medium,
            input.capture.utm_campaign,
            input.capture.utm_content,
            input.capture.utm_term,
            input.capture.gclid,
            input.capture.gbraid,
            input.capture.wbraid,
            input.capture.fbclid,
            input.capture.ttclid,
            input.capture.msclkid,
            input.consentState,
            input.ingestionSource,
            input.ingestionFingerprint,
            JSON.stringify(input.rawPayload),
            input.shopifyCartToken ?? null,
            input.shopifyCheckoutToken ?? null
        ]);
        return {
            id: result.rows[0].id,
            deduplicated: false
        };
    }
    catch (error) {
        if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
            const existing = await findExistingAttributionTouchEventByFingerprint(input.ingestionFingerprint);
            if (existing) {
                return {
                    id: existing.id,
                    deduplicated: true
                };
            }
        }
        throw error;
    }
}
async function ingestAttributionCapture(input, options) {
    const ingestionFingerprint = hashAttributionFingerprint(input.capture, input.eventType, input.ingestionSource);
    if (options.precheckDuplicates) {
        const existing = await findExistingAttributionTouchEventByFingerprint(ingestionFingerprint);
        if (existing) {
            return {
                touchEventId: existing.id,
                capturedAt: existing.captured_at.toISOString(),
                sessionId: existing.roas_radar_session_id,
                deduplicated: true
            };
        }
    }
    const ipHash = hashIp(input.requestIp);
    const result = await withTransaction(async (client) => {
        await upsertSessionAttributionIdentity(client, input.capture);
        await upsertTrackingSessionForCapture(client, input.capture, new Date(input.capture.occurred_at), normalizeNullableString(input.userAgent), ipHash);
        const touchEvent = await insertAttributionTouchEvent(client, {
            capture: input.capture,
            rawPayload: input.rawPayload,
            eventType: input.eventType,
            consentState: input.consentState,
            ingestionSource: input.ingestionSource,
            ingestionFingerprint,
            shopifyCartToken: input.shopifyCartToken,
            shopifyCheckoutToken: input.shopifyCheckoutToken
        });
        await insertTrackingEventForCapture(client, {
            capture: input.capture,
            rawPayload: input.rawPayload,
            eventType: input.eventType,
            consentState: input.consentState,
            ingestionSource: input.ingestionSource,
            ingestionFingerprint,
            shopifyCartToken: input.shopifyCartToken,
            shopifyCheckoutToken: input.shopifyCheckoutToken
        });
        return touchEvent;
    });
    return {
        touchEventId: result.id,
        capturedAt: input.capture.captured_at,
        sessionId: input.capture.roas_radar_session_id,
        deduplicated: result.deduplicated
    };
}
async function ingestTrackingEvent(input, rawPayload, requestIp) {
    const sanitizedInput = sanitizeTrackingInput(input);
    const ingestionFingerprint = hashTrackingFingerprint(sanitizedInput);
    if (sanitizedInput.clientEventId) {
        const existingByClientEventId = await findExistingTrackingEventByClientEventId(sanitizedInput.clientEventId);
        if (existingByClientEventId) {
            return {
                eventId: existingByClientEventId.id,
                ingestedAt: existingByClientEventId.ingested_at.toISOString(),
                sessionId: existingByClientEventId.session_id,
                deduplicated: true,
                sanitizedInput
            };
        }
    }
    const existingByFingerprint = await findExistingTrackingEventByFingerprint(ingestionFingerprint);
    if (existingByFingerprint) {
        return {
            eventId: existingByFingerprint.id,
            ingestedAt: existingByFingerprint.ingested_at.toISOString(),
            sessionId: existingByFingerprint.session_id,
            deduplicated: true,
            sanitizedInput
        };
    }
    const derivedCapture = buildCaptureFromTrackingEvent(sanitizedInput);
    const ipHash = hashIp(requestIp);
    const eventId = await withTransaction(async (client) => {
        await upsertTrackingSessionForCapture(client, {
            ...derivedCapture,
            landing_url: sanitizedInput.pageUrl
        }, new Date(sanitizedInput.occurredAt), sanitizedInput.context.userAgent, ipHash);
        const insertedEventId = await insertTrackingBrowserEvent(client, sanitizedInput, rawPayload, ingestionFingerprint);
        await enqueueAttributionForTrackingTouchpoint(client, {
            sessionId: sanitizedInput.sessionId,
            shopifyCheckoutToken: sanitizedInput.shopifyCheckoutToken,
            shopifyCartToken: sanitizedInput.shopifyCartToken
        });
        await refreshDailyReportingMetrics(client, [
            formatDateInTimezone(new Date(sanitizedInput.occurredAt), await getReportingTimezone(client))
        ]);
        return insertedEventId;
    });
    return {
        eventId,
        ingestedAt: new Date().toISOString(),
        sessionId: sanitizedInput.sessionId,
        deduplicated: false,
        sanitizedInput
    };
}
async function bootstrapSession(rawPayload, pageUrl, landingUrl, referrerUrl, requestIp, userAgent, requestContextSource) {
    const sessionId = randomUUID();
    const now = new Date().toISOString();
    const capture = normalizeAttributionCaptureV1({
        schema_version: ATTRIBUTION_SCHEMA_VERSION,
        roas_radar_session_id: sessionId,
        occurred_at: now,
        captured_at: now,
        landing_url: landingUrl ?? pageUrl,
        referrer_url: referrerUrl,
        page_url: pageUrl,
        ...parseCampaignParameters(landingUrl ?? pageUrl)
    });
    await ingestAttributionCapture({
        capture,
        rawPayload,
        eventType: 'page_view',
        consentState: 'unknown',
        ingestionSource: 'request_query',
        requestIp,
        userAgent
    }, { precheckDuplicates: false });
    return {
        sessionId,
        createdAt: now,
        isNewSession: true,
        requestContextCaptured: requestContextSource !== 'none',
        requestContextSource
    };
}
class InMemoryRateLimiter {
    entries = new Map();
    check(key) {
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
    prune(now) {
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
function enforceAllowedOrigin(req) {
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
function enforceSupportedContentType(req) {
    if (!req.is(['application/json', 'text/plain'])) {
        throw new TrackingHttpError(415, 'unsupported_media_type', 'Tracking endpoint requires application/json or text/plain JSON');
    }
}
function parseTrackingRequestBody(body) {
    if (typeof body === 'string') {
        const trimmed = body.trim();
        if (!trimmed) {
            throw new TrackingHttpError(400, 'invalid_json', 'Request body must not be empty');
        }
        try {
            const parsed = JSON.parse(trimmed);
            if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
                throw new TrackingHttpError(400, 'invalid_json', 'Tracking payload must be a JSON object');
            }
            return parsed;
        }
        catch (error) {
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
function buildSessionBootstrapRawPayload(req) {
    const rawPayload = {};
    if (req.query.pageUrl !== undefined) {
        rawPayload.pageUrl = req.query.pageUrl;
    }
    if (req.query.landingUrl !== undefined) {
        rawPayload.landingUrl = req.query.landingUrl;
    }
    if (req.query.referrerUrl !== undefined) {
        rawPayload.referrerUrl = req.query.referrerUrl;
    }
    const referer = req.header('referer');
    if (referer !== undefined) {
        rawPayload.referer = referer;
    }
    return rawPayload;
}
function enforceRateLimit(req) {
    const requestIp = resolveRequestIp(req);
    const body = typeof req.body === 'object' && req.body !== null ? req.body : {};
    const sessionHint = typeof body.sessionId === 'string' ? body.sessionId : 'anonymous';
    const key = createHash('sha256').update(`${requestIp ?? 'unknown'}:${sessionHint}`).digest('hex');
    const result = trackingRateLimiter.check(key);
    if (!result.allowed) {
        throw new TrackingHttpError(429, 'rate_limit_exceeded', 'Too many tracking requests', {
            retryAfterMs: Math.max(result.resetAt - Date.now(), 0)
        });
    }
}
function parseAttributionCaptureRequest(body) {
    try {
        return attributionCaptureRequestSchema.parse(body);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new TrackingHttpError(400, 'invalid_request', 'Invalid attribution capture payload', error.flatten());
        }
        throw error;
    }
}
function parseTrackingEventRequest(body) {
    try {
        return trackingEventSchema.parse(body);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new TrackingHttpError(400, 'invalid_request', 'Invalid tracking payload', error.flatten());
        }
        throw error;
    }
}
async function emitDerivedAttributionFromBrowserEvent(input, rawPayload, requestIp) {
    try {
        const result = await ingestAttributionCapture({
            capture: buildCaptureFromTrackingEvent(input),
            rawPayload,
            eventType: input.eventType,
            consentState: input.consentState,
            ingestionSource: 'server',
            requestIp,
            userAgent: input.context.userAgent,
            shopifyCartToken: input.shopifyCartToken,
            shopifyCheckoutToken: input.shopifyCheckoutToken
        }, { precheckDuplicates: false });
        return {
            ok: true,
            touchEventId: result.touchEventId,
            deduplicated: result.deduplicated
        };
    }
    catch (error) {
        logError('tracking_dual_write_server_emit_failed', error, {
            sessionId: input.sessionId,
            eventType: input.eventType
        });
        return {
            ok: false,
            touchEventId: null,
            deduplicated: false,
            errorCode: 'server_attribution_emit_failed'
        };
    }
}
export function createTrackingRouter() {
    const router = Router();
    router.get('/session', async (req, res, next) => {
        try {
            enforceAllowedOrigin(req);
            const parsedQuery = sessionBootstrapQuerySchema.parse({
                pageUrl: req.query.pageUrl,
                landingUrl: req.query.landingUrl,
                referrerUrl: req.query.referrerUrl
            });
            const rawPayload = buildSessionBootstrapRawPayload(req);
            const normalizedPageUrl = normalizeTrackingUrl(parsedQuery.pageUrl);
            const normalizedLandingUrl = normalizeTrackingUrl(parsedQuery.landingUrl);
            const headerReferrerUrl = normalizeTrackingUrl(req.header('referer') ?? null);
            const normalizedReferrerUrl = normalizeTrackingUrl(parsedQuery.referrerUrl) ?? headerReferrerUrl;
            if (!normalizedPageUrl) {
                throw new TrackingHttpError(400, 'invalid_request', 'pageUrl must be a valid http(s) URL');
            }
            const requestContextSource = parsedQuery.referrerUrl
                ? 'query'
                : headerReferrerUrl
                    ? 'header'
                    : 'none';
            const result = await bootstrapSession(rawPayload, normalizedPageUrl, normalizedLandingUrl, normalizedReferrerUrl, resolveRequestIp(req), normalizeNullableString(req.header('user-agent')), requestContextSource);
            logAttributionCaptureObserved('session_bootstrap', {
                roas_radar_session_id: result.sessionId,
                landing_url: normalizedLandingUrl ?? normalizedPageUrl,
                referrer_url: normalizedReferrerUrl,
                page_url: normalizedPageUrl
            }, {
                accepted: true,
                deduplicated: !result.isNewSession,
                requestContextCaptured: result.requestContextCaptured,
                requestContextSource: result.requestContextSource
            });
            res.status(200).json({
                ok: true,
                sessionId: result.sessionId,
                createdAt: result.createdAt,
                isNewSession: result.isNewSession
            });
        }
        catch (error) {
            if (error instanceof TrackingHttpError) {
                logWarning('tracking_session_bootstrap_rejected', {
                    code: error.code,
                    statusCode: error.statusCode,
                    details: error.details
                });
            }
            else if (error instanceof z.ZodError) {
                next(new TrackingHttpError(400, 'invalid_request', 'Invalid session bootstrap request', error.flatten()));
                return;
            }
            else {
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
            const rawPayload = parseTrackingRequestBody(req.body);
            const parsed = parseAttributionCaptureRequest(rawPayload);
            const result = await ingestAttributionCapture({
                capture: parsed.capture,
                rawPayload,
                eventType: 'page_view',
                consentState: parsed.consentState,
                ingestionSource: 'server',
                requestIp: resolveRequestIp(req),
                userAgent: normalizeNullableString(req.header('user-agent'))
            }, { precheckDuplicates: true });
            logAttributionCaptureObserved('attribution_capture', parsed.capture, {
                accepted: true,
                deduplicated: result.deduplicated,
                touchEventId: result.touchEventId
            });
            res.status(200).json({
                ok: true,
                sessionId: result.sessionId,
                touchEventId: result.touchEventId,
                capturedAt: result.capturedAt,
                deduplicated: result.deduplicated
            });
        }
        catch (error) {
            if (error instanceof TrackingHttpError) {
                logAttributionCaptureObserved('attribution_capture', req.body, {
                    accepted: false,
                    rejectionCode: error.code,
                    statusCode: error.statusCode
                });
                logWarning('tracking_attribution_rejected', {
                    code: error.code,
                    statusCode: error.statusCode,
                    details: error.details
                });
            }
            else {
                logError('tracking_attribution_failed', error, {
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
            const rawPayload = parseTrackingRequestBody(req.body);
            const input = parseTrackingEventRequest(rawPayload);
            const requestIp = resolveRequestIp(req);
            const browserResult = await ingestTrackingEvent(input, rawPayload, requestIp);
            const serverAttributionResult = await emitDerivedAttributionFromBrowserEvent(browserResult.sanitizedInput, rawPayload, requestIp);
            logAttributionCaptureObserved('browser_event', browserResult.sanitizedInput, {
                accepted: true,
                deduplicated: browserResult.deduplicated,
                eventType: input.eventType
            });
            logInfo('tracking_dual_write_consistency', {
                ...summarizeDualWriteConsistency({
                    browserOutcome: browserResult.deduplicated ? 'deduplicated' : 'accepted',
                    serverOutcome: serverAttributionResult.ok
                        ? serverAttributionResult.deduplicated
                            ? 'deduplicated'
                            : 'accepted'
                        : 'failed'
                }),
                sessionId: browserResult.sessionId,
                eventId: browserResult.eventId,
                eventType: input.eventType,
                touchEventId: serverAttributionResult.touchEventId,
                errorCode: serverAttributionResult.ok ? null : serverAttributionResult.errorCode
            });
            res.status(200).json({
                ok: true,
                eventId: browserResult.eventId,
                ingestedAt: browserResult.ingestedAt,
                sessionId: browserResult.sessionId,
                deduplicated: browserResult.deduplicated,
                attribution: serverAttributionResult
            });
        }
        catch (error) {
            if (error instanceof TrackingHttpError) {
                logWarning('tracking_ingest_rejected', {
                    code: error.code,
                    statusCode: error.statusCode,
                    details: error.details
                });
            }
            else {
                logError('tracking_ingest_failed', error, {
                    path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path
                });
            }
            next(error);
        }
    });
    return router;
}
