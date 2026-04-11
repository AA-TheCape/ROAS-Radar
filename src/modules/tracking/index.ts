import { createHash, randomUUID } from 'node:crypto';

import { Router } from 'express';
import { z } from 'zod';

import { withTransaction } from '../../db/pool.js';

const trackingEventSchema = z.object({
  eventType: z.enum(['page_view', 'product_view', 'add_to_cart', 'checkout_started']),
  occurredAt: z.string().datetime(),
  sessionId: z.string().uuid(),
  pageUrl: z.string().url(),
  referrerUrl: z.string().url().nullable().optional(),
  shopifyCartToken: z.string().nullable().optional(),
  shopifyCheckoutToken: z.string().nullable().optional(),
  clientEventId: z.string().min(1).optional(),
  context: z.object({
    userAgent: z.string().optional(),
    screen: z.string().optional(),
    language: z.string().optional()
  }).default({})
});

type TrackingEventInput = z.infer<typeof trackingEventSchema>;

function parseCampaignParameters(pageUrl: string): Record<string, string | null> {
  const url = new URL(pageUrl);

  return {
    utm_source: url.searchParams.get('utm_source'),
    utm_medium: url.searchParams.get('utm_medium'),
    utm_campaign: url.searchParams.get('utm_campaign'),
    utm_content: url.searchParams.get('utm_content'),
    utm_term: url.searchParams.get('utm_term'),
    gclid: url.searchParams.get('gclid'),
    fbclid: url.searchParams.get('fbclid'),
    ttclid: url.searchParams.get('ttclid'),
    msclkid: url.searchParams.get('msclkid')
  };
}

function hashIp(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  return createHash('sha256').update(value).digest('hex');
}

async function ingestTrackingEvent(input: TrackingEventInput, requestIp: string | undefined): Promise<{
  eventId: string;
  ingestedAt: string;
  sessionId: string;
}> {
  const params = parseCampaignParameters(input.pageUrl);
  const eventId = randomUUID();
  const occurredAt = new Date(input.occurredAt);
  const ingestedAt = new Date().toISOString();
  const ipHash = hashIp(requestIp);
  const userAgent = input.context.userAgent ?? null;

  await withTransaction(async (client) => {
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
        params.utm_source,
        params.utm_medium,
        params.utm_campaign,
        params.utm_content,
        params.utm_term,
        params.gclid,
        params.fbclid,
        params.ttclid,
        params.msclkid,
        userAgent,
        ipHash
      ]
    );

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
          $19::jsonb
        )
        ON CONFLICT DO NOTHING
      `,
      [
        eventId,
        input.sessionId,
        input.eventType,
        occurredAt,
        input.pageUrl,
        input.referrerUrl ?? null,
        params.utm_source,
        params.utm_medium,
        params.utm_campaign,
        params.utm_content,
        params.utm_term,
        params.gclid,
        params.fbclid,
        params.ttclid,
        params.msclkid,
        input.shopifyCartToken ?? null,
        input.shopifyCheckoutToken ?? null,
        input.clientEventId ?? null,
        JSON.stringify(input)
      ]
    );
  });

  return {
    eventId,
    ingestedAt,
    sessionId: input.sessionId
  };
}

export function createTrackingRouter(): Router {
  const router = Router();

  router.post('/', async (req, res, next) => {
    try {
      const input = trackingEventSchema.parse(req.body);
      const result = await ingestTrackingEvent(input, req.ip);

      res.status(200).json({
        ok: true,
        ...result
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
