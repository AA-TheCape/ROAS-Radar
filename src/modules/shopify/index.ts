import { createHash, createHmac, timingSafeEqual } from 'node:crypto';

import { type RequestHandler, Router } from 'express';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { hashIdentityEmail, stitchKnownCustomerIdentity } from '../identity/index.js';

const shopifyAttributeSchema = z.object({
  name: z.string().optional(),
  value: z.union([z.string(), z.number(), z.boolean()]).nullable().optional()
});

const shopifyCustomerSchema = z
  .object({
    id: z.union([z.string(), z.number()]).optional(),
    email: z.string().email().nullable().optional(),
    phone: z.string().nullable().optional()
  })
  .nullable()
  .optional();

const shopifyOrderPayloadSchema = z.object({
  id: z.union([z.string(), z.number()]),
  order_number: z.union([z.string(), z.number()]).optional(),
  customer: shopifyCustomerSchema,
  email: z.string().email().nullable().optional(),
  currency: z.string().min(1).optional(),
  subtotal_price: z.union([z.string(), z.number()]).optional(),
  total_price: z.union([z.string(), z.number()]).optional(),
  financial_status: z.string().nullable().optional(),
  fulfillment_status: z.string().nullable().optional(),
  processed_at: z.string().datetime().nullable().optional(),
  created_at: z.string().datetime().nullable().optional(),
  updated_at: z.string().datetime().nullable().optional(),
  checkout_token: z.string().nullable().optional(),
  cart_token: z.string().nullable().optional(),
  source_name: z.string().nullable().optional(),
  note_attributes: z.array(shopifyAttributeSchema).optional(),
  attributes: z.array(shopifyAttributeSchema).optional()
});

type ShopifyOrderPayload = z.infer<typeof shopifyOrderPayloadSchema>;

type WebhookReceiptRow = {
  id: number;
  status: string;
  processed_at: Date | null;
};

type PersistWebhookInput = {
  payload: ShopifyOrderPayload;
  rawBody: Buffer;
  shopDomain: string;
  topic: string;
  webhookId: string | null;
};

function verifyWebhookSignature(rawBody: Buffer, signature: string | undefined): boolean {
  if (!env.SHOPIFY_WEBHOOK_SECRET || !signature) {
    return false;
  }

  const digest = createHmac('sha256', env.SHOPIFY_WEBHOOK_SECRET).update(rawBody).digest('base64');

  if (digest.length !== signature.length) {
    return false;
  }

  return timingSafeEqual(Buffer.from(digest), Buffer.from(signature));
}

function getAttributeValue(
  attributes: Array<z.infer<typeof shopifyAttributeSchema>> | undefined,
  key: string
): string | null {
  const match = attributes?.find((attribute) => attribute.name === key);

  if (match?.value === undefined || match.value === null) {
    return null;
  }

  const normalized = String(match.value).trim();
  return normalized ? normalized : null;
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

async function createOrReuseWebhookReceipt(
  topic: string,
  shopDomain: string,
  webhookId: string | null,
  payloadHash: string,
  payload: ShopifyOrderPayload
): Promise<WebhookReceiptRow> {
  const insertResult = await query<WebhookReceiptRow>(
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
      RETURNING id, status, processed_at
    `,
    [topic, shopDomain, webhookId, payloadHash, JSON.stringify(payload)]
  );

  if (insertResult.rowCount) {
    return insertResult.rows[0];
  }

  const existingResult = await query<WebhookReceiptRow>(
    `
      SELECT
        id,
        status,
        processed_at
      FROM shopify_webhook_receipts
      WHERE (
        $1::text IS NOT NULL
        AND webhook_id = $1
      ) OR (
        topic = $2
        AND shop_domain = $3
        AND payload_hash = $4
      )
      ORDER BY id DESC
      LIMIT 1
    `,
    [webhookId, topic, shopDomain, payloadHash]
  );

  if (!existingResult.rowCount) {
    throw new Error('Failed to create Shopify webhook receipt.');
  }

  return existingResult.rows[0];
}

async function markWebhookReceiptStatus(receiptId: number, status: 'processed' | 'failed'): Promise<void> {
  await query(
    `
      UPDATE shopify_webhook_receipts
      SET
        status = $2,
        processed_at = CASE WHEN $2 = 'processed' THEN now() ELSE processed_at END
      WHERE id = $1
    `,
    [receiptId, status]
  );
}

async function normalizeShopifyOrder(receiptId: number, payload: ShopifyOrderPayload): Promise<void> {
  const landingSessionId =
    getAttributeValue(payload.note_attributes, 'roas_radar_session_id') ??
    getAttributeValue(payload.attributes, 'roas_radar_session_id');
  const shopifyCustomerId = payload.customer?.id ? String(payload.customer.id) : null;
  const normalizedOrderEmail = payload.email ?? payload.customer?.email ?? null;
  const orderEmailHash = hashIdentityEmail(normalizedOrderEmail);

  await withTransaction(async (client) => {
    if (shopifyCustomerId) {
      await client.query(
        `
          INSERT INTO shopify_customers (
            shopify_customer_id,
            email,
            email_hash,
            phone,
            created_at,
            updated_at
          )
          VALUES ($1, $2, $3, $4, now(), now())
          ON CONFLICT (shopify_customer_id)
          DO UPDATE SET
            email = COALESCE(EXCLUDED.email, shopify_customers.email),
            email_hash = COALESCE(EXCLUDED.email_hash, shopify_customers.email_hash),
            phone = COALESCE(EXCLUDED.phone, shopify_customers.phone),
            updated_at = now()
        `,
        [shopifyCustomerId, normalizedOrderEmail, orderEmailHash, payload.customer?.phone ?? null]
      );
    }

    await client.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          shopify_order_number,
          shopify_customer_id,
          email,
          email_hash,
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
          $13,
          $14::uuid,
          $15,
          $16,
          $17,
          $18::jsonb,
          now()
        )
        ON CONFLICT (shopify_order_id)
        DO UPDATE SET
          shopify_order_number = COALESCE(EXCLUDED.shopify_order_number, shopify_orders.shopify_order_number),
          shopify_customer_id = COALESCE(EXCLUDED.shopify_customer_id, shopify_orders.shopify_customer_id),
          email = COALESCE(EXCLUDED.email, shopify_orders.email),
          email_hash = COALESCE(EXCLUDED.email_hash, shopify_orders.email_hash),
          currency_code = EXCLUDED.currency_code,
          subtotal_price = EXCLUDED.subtotal_price,
          total_price = EXCLUDED.total_price,
          financial_status = COALESCE(EXCLUDED.financial_status, shopify_orders.financial_status),
          fulfillment_status = COALESCE(EXCLUDED.fulfillment_status, shopify_orders.fulfillment_status),
          processed_at = COALESCE(EXCLUDED.processed_at, shopify_orders.processed_at),
          created_at_shopify = COALESCE(EXCLUDED.created_at_shopify, shopify_orders.created_at_shopify),
          updated_at_shopify = COALESCE(EXCLUDED.updated_at_shopify, shopify_orders.updated_at_shopify),
          landing_session_id = COALESCE(EXCLUDED.landing_session_id, shopify_orders.landing_session_id),
          checkout_token = COALESCE(EXCLUDED.checkout_token, shopify_orders.checkout_token),
          cart_token = COALESCE(EXCLUDED.cart_token, shopify_orders.cart_token),
          source_name = COALESCE(EXCLUDED.source_name, shopify_orders.source_name),
          raw_payload = EXCLUDED.raw_payload,
          ingested_at = now()
      `,
      [
        String(payload.id),
        payload.order_number ? String(payload.order_number) : null,
        shopifyCustomerId,
        normalizedOrderEmail,
        orderEmailHash,
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

    await stitchKnownCustomerIdentity(client, {
      shopifyOrderId: String(payload.id),
      shopifyCustomerId,
      email: normalizedOrderEmail,
      landingSessionId: toNullableUuid(landingSessionId),
      checkoutToken: payload.checkout_token ?? null,
      cartToken: payload.cart_token ?? null
    });

    await client.query(
      `
        UPDATE shopify_webhook_receipts
        SET
          status = 'processed',
          processed_at = now()
        WHERE id = $1
      `,
      [receiptId]
    );
  });
}

async function persistWebhook(input: PersistWebhookInput): Promise<{ duplicated: boolean }> {
  const payloadHash = createHash('sha256').update(input.rawBody).digest('hex');
  const receipt = await createOrReuseWebhookReceipt(
    input.topic,
    input.shopDomain,
    input.webhookId,
    payloadHash,
    input.payload
  );

  if (receipt.status === 'processed') {
    return { duplicated: true };
  }

  try {
    await normalizeShopifyOrder(receipt.id, input.payload);
    return { duplicated: false };
  } catch (error) {
    await markWebhookReceiptStatus(receipt.id, 'failed');
    throw error;
  }
}

function createOrderWebhookHandler(defaultTopic: string): RequestHandler {
  return async (req, res, next) => {
    try {
      const rawBody = req.body as Buffer;
      const signature = req.header('x-shopify-hmac-sha256') ?? undefined;

      if (!Buffer.isBuffer(rawBody)) {
        res.status(400).json({ error: 'Expected a raw Shopify webhook payload.' });
        return;
      }

      if (!verifyWebhookSignature(rawBody, signature)) {
        res.status(401).json({ error: 'Invalid Shopify webhook signature.' });
        return;
      }

      const payload = shopifyOrderPayloadSchema.parse(JSON.parse(rawBody.toString('utf8')));
      await persistWebhook({
        payload,
        rawBody,
        topic: req.header('x-shopify-topic') ?? defaultTopic,
        shopDomain: req.header('x-shopify-shop-domain') ?? 'unknown',
        webhookId: req.header('x-shopify-webhook-id') ?? null
      });

      res.status(200).json({ ok: true });
    } catch (error) {
      next(error);
    }
  };
}

export function createShopifyRouter(): Router {
  const router = Router();

  router.post('/orders-create', createOrderWebhookHandler('orders/create'));
  router.post('/orders-paid', createOrderWebhookHandler('orders/paid'));

  return router;
}
