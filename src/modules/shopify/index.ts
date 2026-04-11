import { createHash, createHmac, timingSafeEqual } from 'node:crypto';

import { Router } from 'express';

import { env } from '../../config/env.js';
import { withTransaction } from '../../db/pool.js';

type ShopifyOrderPayload = {
  id: number | string;
  order_number?: number | string;
  customer?: {
    id?: number | string;
    email?: string | null;
    phone?: string | null;
  } | null;
  email?: string | null;
  currency?: string;
  subtotal_price?: string | number;
  total_price?: string | number;
  financial_status?: string | null;
  fulfillment_status?: string | null;
  processed_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  checkout_token?: string | null;
  cart_token?: string | null;
  source_name?: string | null;
  note_attributes?: Array<{ name?: string; value?: string | null }>;
  attributes?: Array<{ name?: string; value?: string | null }>;
};

function verifyWebhookSignature(rawBody: Buffer, signature: string | undefined): boolean {
  if (!env.SHOPIFY_WEBHOOK_SECRET || !signature) {
    return false;
  }

  const digest = createHmac('sha256', env.SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody)
    .digest('base64');

  if (digest.length !== signature.length) {
    return false;
  }

  return timingSafeEqual(Buffer.from(digest), Buffer.from(signature));
}

function getAttributeValue(
  attributes: Array<{ name?: string; value?: string | null }> | undefined,
  key: string
): string | null {
  const match = attributes?.find((attribute) => attribute.name === key);
  return match?.value ?? null;
}

function toNumericString(value: string | number | undefined): string {
  if (value === undefined) {
    return '0';
  }

  return String(value);
}

function toNullableUuid(value: string | null): string | null {
  if (!value) {
    return null;
  }

  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
    ? value
    : null;
}

async function persistWebhook(topic: string, shopDomain: string, webhookId: string | null, payload: ShopifyOrderPayload): Promise<void> {
  const payloadHash = createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  const landingSessionId =
    getAttributeValue(payload.note_attributes, 'roas_radar_session_id') ??
    getAttributeValue(payload.attributes, 'roas_radar_session_id');
  const shopifyCustomerId = payload.customer?.id ? String(payload.customer.id) : null;

  await withTransaction(async (client) => {
    const receiptInsert = await client.query(
      `
        INSERT INTO shopify_webhook_receipts (
          topic,
          shop_domain,
          webhook_id,
          payload_hash,
          received_at,
          status,
          raw_payload
        )
        VALUES ($1, $2, $3, $4, now(), 'received', $5::jsonb)
        ON CONFLICT DO NOTHING
        RETURNING id
      `,
      [topic, shopDomain, webhookId, payloadHash, JSON.stringify(payload)]
    );

    if (webhookId && receiptInsert.rowCount === 0) {
      return;
    }

    if (shopifyCustomerId) {
      await client.query(
        `
          INSERT INTO shopify_customers (
            shopify_customer_id,
            email,
            phone,
            created_at,
            updated_at
          )
          VALUES ($1, $2, $3, now(), now())
          ON CONFLICT (shopify_customer_id)
          DO UPDATE SET
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            updated_at = now()
        `,
        [shopifyCustomerId, payload.customer?.email ?? payload.email ?? null, payload.customer?.phone ?? null]
      );
    }

    await client.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          shopify_order_number,
          shopify_customer_id,
          email,
          currency_code,
          subtotal_price,
          total_price,
          financial_status,
          fulfillment_status,
          processed_at,
          created_at_shopify,
          updated_at_shopify,
          landing_session_id,
          checkout_token,
          cart_token,
          source_name,
          raw_payload,
          ingested_at
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
          $13::uuid,
          $14,
          $15,
          $16,
          $17::jsonb,
          now()
        )
        ON CONFLICT (shopify_order_id)
        DO UPDATE SET
          shopify_order_number = EXCLUDED.shopify_order_number,
          shopify_customer_id = EXCLUDED.shopify_customer_id,
          email = EXCLUDED.email,
          currency_code = EXCLUDED.currency_code,
          subtotal_price = EXCLUDED.subtotal_price,
          total_price = EXCLUDED.total_price,
          financial_status = EXCLUDED.financial_status,
          fulfillment_status = EXCLUDED.fulfillment_status,
          processed_at = EXCLUDED.processed_at,
          created_at_shopify = EXCLUDED.created_at_shopify,
          updated_at_shopify = EXCLUDED.updated_at_shopify,
          landing_session_id = COALESCE(EXCLUDED.landing_session_id, shopify_orders.landing_session_id),
          checkout_token = COALESCE(EXCLUDED.checkout_token, shopify_orders.checkout_token),
          cart_token = COALESCE(EXCLUDED.cart_token, shopify_orders.cart_token),
          source_name = EXCLUDED.source_name,
          raw_payload = EXCLUDED.raw_payload,
          ingested_at = now()
      `,
      [
        String(payload.id),
        payload.order_number ? String(payload.order_number) : null,
        shopifyCustomerId,
        payload.email ?? payload.customer?.email ?? null,
        payload.currency ?? 'USD',
        toNumericString(payload.subtotal_price),
        toNumericString(payload.total_price),
        payload.financial_status ?? null,
        payload.fulfillment_status ?? null,
        payload.processed_at ?? null,
        payload.created_at ?? null,
        payload.updated_at ?? null,
        toNullableUuid(landingSessionId),
        payload.checkout_token ?? null,
        payload.cart_token ?? null,
        payload.source_name ?? null,
        JSON.stringify(payload)
      ]
    );

    await client.query(
      `
        UPDATE shopify_webhook_receipts
        SET
          status = 'processed',
          processed_at = now()
        WHERE topic = $1
          AND shop_domain = $2
          AND payload_hash = $3
      `,
      [topic, shopDomain, payloadHash]
    );
  });
}

export function createShopifyRouter(): Router {
  const router = Router();

  router.post('/orders-create', async (req, res, next) => {
    try {
      const rawBody = req.body as Buffer;
      const signature = req.header('x-shopify-hmac-sha256') ?? undefined;

      if (!verifyWebhookSignature(rawBody, signature)) {
        res.status(401).json({ error: 'Invalid Shopify webhook signature.' });
        return;
      }

      const payload = JSON.parse(rawBody.toString('utf8')) as ShopifyOrderPayload;
      await persistWebhook(
        req.header('x-shopify-topic') ?? 'orders/create',
        req.header('x-shopify-shop-domain') ?? 'unknown',
        req.header('x-shopify-webhook-id'),
        payload
      );

      res.status(200).json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  router.post('/orders-paid', async (req, res, next) => {
    try {
      const rawBody = req.body as Buffer;
      const signature = req.header('x-shopify-hmac-sha256') ?? undefined;

      if (!verifyWebhookSignature(rawBody, signature)) {
        res.status(401).json({ error: 'Invalid Shopify webhook signature.' });
        return;
      }

      const payload = JSON.parse(rawBody.toString('utf8')) as ShopifyOrderPayload;
      await persistWebhook(
        req.header('x-shopify-topic') ?? 'orders/paid',
        req.header('x-shopify-shop-domain') ?? 'unknown',
        req.header('x-shopify-webhook-id'),
        payload
      );

      res.status(200).json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
