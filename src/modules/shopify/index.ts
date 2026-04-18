import { createHash, createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

import { type RequestHandler, Router } from 'express';
import { type PoolClient } from 'pg';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo, logWarning } from '../../observability/index.js';
import { enqueueAttributionForOrder } from '../attribution/index.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
import { hashIdentityEmail, stitchKnownCustomerIdentity } from '../identity/index.js';

const OAUTH_STATE_TTL_MINUTES = 10;

const SHOPIFY_WEBHOOK_TOPICS = [
  {
    topic: 'ORDERS_CREATE',
    deliveryPath: '/orders-create'
  },
  {
    topic: 'ORDERS_PAID',
    deliveryPath: '/orders-paid'
  },
  {
    topic: 'APP_UNINSTALLED',
    deliveryPath: '/app-uninstalled'
  }
] as const;

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

const shopifyLineItemSchema = z.object({
  id: z.union([z.string(), z.number()]).optional(),
  product_id: z.union([z.string(), z.number()]).nullable().optional(),
  variant_id: z.union([z.string(), z.number()]).nullable().optional(),
  sku: z.string().nullable().optional(),
  title: z.string().nullable().optional(),
  name: z.string().nullable().optional(),
  vendor: z.string().nullable().optional(),
  quantity: z.coerce.number().int().optional(),
  price: z.union([z.string(), z.number()]).optional(),
  total_discount: z.union([z.string(), z.number()]).optional(),
  fulfillment_status: z.string().nullable().optional(),
  requires_shipping: z.boolean().nullable().optional(),
  taxable: z.boolean().nullable().optional()
});

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
  processed_at: z.string().datetime({ offset: true }).nullable().optional(),
  created_at: z.string().datetime({ offset: true }).nullable().optional(),
  updated_at: z.string().datetime({ offset: true }).nullable().optional(),
  checkout_token: z.string().nullable().optional(),
  cart_token: z.string().nullable().optional(),
  source_name: z.string().nullable().optional(),
  line_items: z.array(shopifyLineItemSchema).default([]),
  note_attributes: z.array(shopifyAttributeSchema).optional(),
  attributes: z.array(shopifyAttributeSchema).optional()
});

const installRequestSchema = z.object({
  shop: z.string().min(1),
  returnTo: z.string().optional()
});

const callbackQuerySchema = z.object({
  shop: z.string().min(1),
  code: z.string().min(1),
  hmac: z.string().min(1),
  state: z.string().min(1)
});

type ShopifyOrderPayload = z.infer<typeof shopifyOrderPayloadSchema>;
type ShopifyLineItemPayload = z.infer<typeof shopifyLineItemSchema>;

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

type ShopifyInstallStateRow = {
  id: string;
  return_to: string | null;
};

type ShopifyInstallationSummaryRow = {
  shop_domain: string;
  status: string;
  installed_at: Date;
  reconnected_at: Date | null;
  uninstalled_at: Date | null;
  scopes: string[];
  webhook_base_url: string;
  webhook_subscriptions: unknown;
  shop_name: string | null;
  shop_email: string | null;
  shop_currency: string | null;
  updated_at: Date;
};

type ShopifyShopIdentity = {
  myshopifyDomain: string;
  name: string | null;
  email: string | null;
  currencyCode: string | null;
  raw: unknown;
};

type ShopifyWebhookSubscription = {
  topic: string;
  id: string;
  callbackUrl: string | null;
};

class ShopifyHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'ShopifyHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function getShopifySharedSecret(): string {
  return env.SHOPIFY_APP_API_SECRET || env.SHOPIFY_WEBHOOK_SECRET;
}

function normalizeShopDomain(rawShop: string): string {
  const normalized = rawShop.trim().toLowerCase();

  if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(normalized)) {
    throw new ShopifyHttpError(400, 'invalid_shop_domain', 'shop must be a valid *.myshopify.com domain');
  }

  return normalized;
}

function normalizeReturnTo(rawReturnTo: string | undefined): string | null {
  if (!rawReturnTo) {
    return null;
  }

  const trimmed = rawReturnTo.trim();

  if (!trimmed) {
    return null;
  }

  if (trimmed.startsWith('/')) {
    return trimmed;
  }

  try {
    const url = new URL(trimmed);

    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error('Unsupported redirect protocol');
    }

    return url.toString();
  } catch {
    throw new ShopifyHttpError(400, 'invalid_return_to', 'returnTo must be an absolute http(s) URL or a root-relative path');
  }
}

function assertShopifyAppConfig(): void {
  const missing = [
    ['SHOPIFY_APP_API_KEY', env.SHOPIFY_APP_API_KEY],
    ['SHOPIFY_APP_API_SECRET', env.SHOPIFY_APP_API_SECRET],
    ['SHOPIFY_APP_API_VERSION', env.SHOPIFY_APP_API_VERSION],
    ['SHOPIFY_APP_BASE_URL', env.SHOPIFY_APP_BASE_URL],
    ['SHOPIFY_APP_ENCRYPTION_KEY', env.SHOPIFY_APP_ENCRYPTION_KEY]
  ]
    .filter(([, value]) => !value)
    .map(([key]) => key);

  if (missing.length > 0) {
    throw new ShopifyHttpError(
      500,
      'shopify_app_config_missing',
      `Missing Shopify app configuration: ${missing.join(', ')}`
    );
  }
}

function getAppBaseUrl(): string {
  const url = new URL(env.SHOPIFY_APP_BASE_URL);
  return url.toString().replace(/\/$/, '');
}

function buildOAuthCallbackUrl(): string {
  return `${getAppBaseUrl()}/shopify/oauth/callback`;
}

function createOAuthStateDigest(state: string): string {
  return createHash('sha256').update(state).digest('hex');
}

function verifyWebhookSignature(rawBody: Buffer, signature: string | undefined): boolean {
  const secret = getShopifySharedSecret();

  if (!secret || !signature) {
    return false;
  }

  const digest = createHmac('sha256', secret).update(rawBody).digest('base64');

  if (digest.length !== signature.length) {
    return false;
  }

  return timingSafeEqual(Buffer.from(digest), Buffer.from(signature));
}

function createOAuthHmacMessage(originalUrl: string): string {
  const queryIndex = originalUrl.indexOf('?');

  if (queryIndex === -1) {
    return '';
  }

  const params = new URLSearchParams(originalUrl.slice(queryIndex + 1));
  const pairs: string[] = [];

  for (const [key, value] of params.entries()) {
    if (key === 'hmac' || key === 'signature') {
      continue;
    }

    pairs.push(`${key}=${value}`);
  }

  return pairs.sort().join('&');
}

function verifyShopifyOAuthHmac(originalUrl: string, providedHmac: string): boolean {
  const secret = env.SHOPIFY_APP_API_SECRET;

  if (!secret || !providedHmac) {
    return false;
  }

  const digest = createHmac('sha256', secret).update(createOAuthHmacMessage(originalUrl)).digest('hex');

  if (digest.length !== providedHmac.length) {
    return false;
  }

  return timingSafeEqual(Buffer.from(digest), Buffer.from(providedHmac));
}

function buildShopifyInstallUrl(shopDomain: string, state: string, returnTo: string | null): string {
  assertShopifyAppConfig();

  const url = new URL(`https://${shopDomain}/admin/oauth/authorize`);
  url.searchParams.set('client_id', env.SHOPIFY_APP_API_KEY);
  url.searchParams.set('scope', env.SHOPIFY_APP_SCOPES.join(','));
  url.searchParams.set('redirect_uri', buildOAuthCallbackUrl());
  url.searchParams.set('state', state);

  if (returnTo) {
    url.searchParams.set('return_to', returnTo);
  }

  return url.toString();
}

async function assertSingleStoreInstallAllowed(shopDomain: string): Promise<void> {
  const result = await query<{ shop_domain: string; status: string }>(
    `
      SELECT shop_domain, status
      FROM shopify_app_installations
      WHERE shop_domain <> $1
        AND status <> 'uninstalled'
      ORDER BY updated_at DESC
      LIMIT 1
    `,
    [shopDomain]
  );

  if (result.rowCount) {
    throw new ShopifyHttpError(
      409,
      'shopify_store_already_connected',
      `A different Shopify store is already connected for this MVP environment: ${result.rows[0].shop_domain}`
    );
  }
}

async function persistOAuthState(shopDomain: string, state: string, returnTo: string | null): Promise<void> {
  await query(
    `
      INSERT INTO shopify_oauth_states (
        shop_domain,
        state_digest,
        return_to,
        expires_at
      )
      VALUES ($1, $2, $3, now() + ($4 || ' minutes')::interval)
    `,
    [shopDomain, createOAuthStateDigest(state), returnTo, String(OAUTH_STATE_TTL_MINUTES)]
  );
}

async function consumeOAuthState(shopDomain: string, state: string): Promise<ShopifyInstallStateRow> {
  return withTransaction(async (client) => {
    const stateResult = await client.query<ShopifyInstallStateRow>(
      `
        SELECT id, return_to
        FROM shopify_oauth_states
        WHERE shop_domain = $1
          AND state_digest = $2
          AND consumed_at IS NULL
          AND expires_at > now()
        ORDER BY created_at DESC
        LIMIT 1
        FOR UPDATE
      `,
      [shopDomain, createOAuthStateDigest(state)]
    );

    if (!stateResult.rowCount) {
      throw new ShopifyHttpError(400, 'invalid_oauth_state', 'OAuth state is invalid or expired');
    }

    await client.query(
      `
        UPDATE shopify_oauth_states
        SET consumed_at = now()
        WHERE id = $1
      `,
      [stateResult.rows[0].id]
    );

    return stateResult.rows[0];
  });
}

async function exchangeCodeForAccessToken(shopDomain: string, code: string): Promise<{ accessToken: string; scopes: string[] }> {
  const response = await fetch(`https://${shopDomain}/admin/oauth/access_token`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json'
    },
    body: JSON.stringify({
      client_id: env.SHOPIFY_APP_API_KEY,
      client_secret: env.SHOPIFY_APP_API_SECRET,
      code
    })
  });

  const payload = (await response.json()) as {
    access_token?: string;
    scope?: string;
    error?: string;
    error_description?: string;
  };

  if (!response.ok || !payload.access_token) {
    throw new ShopifyHttpError(502, 'shopify_token_exchange_failed', 'Shopify OAuth token exchange failed', payload);
  }

  return {
    accessToken: payload.access_token,
    scopes: payload.scope
      ? payload.scope
          .split(',')
          .map((value) => value.trim())
          .filter(Boolean)
      : []
  };
}

async function callShopifyAdminGraphql<TData>(
  shopDomain: string,
  accessToken: string,
  graphqlQuery: string,
  variables?: Record<string, unknown>
): Promise<TData> {
  const response = await fetch(`https://${shopDomain}/admin/api/${env.SHOPIFY_APP_API_VERSION}/graphql.json`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json',
      'x-shopify-access-token': accessToken
    },
    body: JSON.stringify({
      query: graphqlQuery,
      variables: variables ?? {}
    })
  });

  const payload = (await response.json()) as {
    data?: TData;
    errors?: Array<{ message: string }>;
  };

  if (!response.ok || payload.errors?.length || !payload.data) {
    throw new ShopifyHttpError(502, 'shopify_admin_api_failed', 'Shopify Admin API request failed', payload);
  }

  return payload.data;
}

async function fetchShopIdentity(shopDomain: string, accessToken: string): Promise<ShopifyShopIdentity> {
  const data = await callShopifyAdminGraphql<{
    shop: {
      name: string | null;
      email: string | null;
      currencyCode: string | null;
      myshopifyDomain: string;
    };
  }>(
    shopDomain,
    accessToken,
    `
      query ConnectedShop {
        shop {
          name
          email
          currencyCode
          myshopifyDomain
        }
      }
    `
  );

  return {
    myshopifyDomain: data.shop.myshopifyDomain,
    name: data.shop.name,
    email: data.shop.email,
    currencyCode: data.shop.currencyCode,
    raw: data.shop
  };
}

async function provisionWebhookSubscriptions(
  shopDomain: string,
  accessToken: string,
  webhookBaseUrl: string
): Promise<ShopifyWebhookSubscription[]> {
  const subscriptions: ShopifyWebhookSubscription[] = [];

  for (const subscription of SHOPIFY_WEBHOOK_TOPICS) {
    const callbackUrl = `${webhookBaseUrl}${subscription.deliveryPath}`;
    const data = await callShopifyAdminGraphql<{
      webhookSubscriptionCreate: {
        userErrors: Array<{ field: string[] | null; message: string }>;
        webhookSubscription: {
          id: string;
          topic: string;
          endpoint: {
            __typename: string;
            callbackUrl?: string | null;
          } | null;
        } | null;
      };
    }>(
      shopDomain,
      accessToken,
      `
        mutation RegisterWebhook($topic: WebhookSubscriptionTopic!, $callbackUrl: URL!) {
          webhookSubscriptionCreate(
            topic: $topic
            webhookSubscription: {
              callbackUrl: $callbackUrl
              format: JSON
            }
          ) {
            userErrors {
              field
              message
            }
            webhookSubscription {
              id
              topic
              endpoint {
                __typename
                ... on WebhookHttpEndpoint {
                  callbackUrl
                }
              }
            }
          }
        }
      `,
      {
        topic: subscription.topic,
        callbackUrl
      }
    );

    if (data.webhookSubscriptionCreate.userErrors.length > 0 || !data.webhookSubscriptionCreate.webhookSubscription) {
      throw new ShopifyHttpError(
        502,
        'shopify_webhook_registration_failed',
        `Failed to register Shopify webhook for ${subscription.topic}`,
        data.webhookSubscriptionCreate.userErrors
      );
    }

    subscriptions.push({
      topic: data.webhookSubscriptionCreate.webhookSubscription.topic,
      id: data.webhookSubscriptionCreate.webhookSubscription.id,
      callbackUrl: data.webhookSubscriptionCreate.webhookSubscription.endpoint?.callbackUrl ?? null
    });
  }

  return subscriptions;
}

async function upsertShopifyInstallation(params: {
  shopDomain: string;
  accessToken: string;
  scopes: string[];
  webhookBaseUrl: string;
  webhookSubscriptions: ShopifyWebhookSubscription[];
  shopIdentity: ShopifyShopIdentity;
}): Promise<void> {
  await query(
    `
      INSERT INTO shopify_app_installations (
        shop_domain,
        access_token_encrypted,
        scopes,
        status,
        installed_at,
        webhook_base_url,
        webhook_subscriptions,
        shop_name,
        shop_email,
        shop_currency,
        raw_shop_data,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        $4::text[],
        'active',
        now(),
        $5,
        $6::jsonb,
        $7,
        $8,
        $9,
        $10::jsonb,
        now(),
        now()
      )
      ON CONFLICT (shop_domain)
      DO UPDATE SET
        access_token_encrypted = pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        scopes = $4::text[],
        status = 'active',
        reconnected_at = now(),
        uninstalled_at = NULL,
        webhook_base_url = $5,
        webhook_subscriptions = $6::jsonb,
        shop_name = $7,
        shop_email = $8,
        shop_currency = $9,
        raw_shop_data = $10::jsonb,
        updated_at = now()
    `,
    [
      params.shopDomain,
      params.accessToken,
      env.SHOPIFY_APP_ENCRYPTION_KEY,
      params.scopes,
      params.webhookBaseUrl,
      JSON.stringify(params.webhookSubscriptions),
      params.shopIdentity.name,
      params.shopIdentity.email,
      params.shopIdentity.currencyCode,
      JSON.stringify(params.shopIdentity.raw)
    ]
  );
}

async function getShopifyInstallationSummary(): Promise<ShopifyInstallationSummaryRow | null> {
  const result = await query<ShopifyInstallationSummaryRow>(
    `
      SELECT
        shop_domain,
        status,
        installed_at,
        reconnected_at,
        uninstalled_at,
        scopes,
        webhook_base_url,
        webhook_subscriptions,
        shop_name,
        shop_email,
        shop_currency,
        updated_at
      FROM shopify_app_installations
      ORDER BY updated_at DESC
      LIMIT 1
    `
  );

  return result.rows[0] ?? null;
}

async function getActiveShopifyAccessToken(shopDomain: string): Promise<string> {
  const result = await query<{ access_token: string }>(
    `
      SELECT
        pgp_sym_decrypt(access_token_encrypted, $2) AS access_token
      FROM shopify_app_installations
      WHERE shop_domain = $1
        AND status = 'active'
      LIMIT 1
    `,
    [shopDomain, env.SHOPIFY_APP_ENCRYPTION_KEY]
  );

  if (!result.rowCount) {
    throw new ShopifyHttpError(404, 'shopify_installation_not_found', 'No active Shopify installation was found');
  }

  return result.rows[0].access_token;
}

async function getActiveInstalledShopDomain(): Promise<string | null> {
  const result = await query<{ shop_domain: string }>(
    `
      SELECT shop_domain
      FROM shopify_app_installations
      WHERE status = 'active'
      ORDER BY updated_at DESC
      LIMIT 1
    `
  );

  return result.rows[0]?.shop_domain ?? null;
}

async function markInstallationUninstalled(shopDomain: string): Promise<void> {
  await query(
    `
      UPDATE shopify_app_installations
      SET
        status = 'uninstalled',
        uninstalled_at = now(),
        updated_at = now()
      WHERE shop_domain = $1
    `,
    [shopDomain]
  );
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

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function isEligibleOnlineStoreOrder(sourceName: string | null | undefined): boolean {
  const normalizedSource = normalizeNullableString(sourceName)?.toLowerCase();
  return normalizedSource === 'web';
}

function buildLineItemExternalId(orderId: string, lineItem: ShopifyLineItemPayload, index: number): string {
  if (lineItem.id !== undefined && lineItem.id !== null) {
    return String(lineItem.id);
  }

  return `${orderId}:line:${index + 1}`;
}

function normalizeLineItemText(value: string | null | undefined): string | null {
  return normalizeNullableString(value);
}

async function resolveLandingSessionId(
  client: PoolClient,
  payload: ShopifyOrderPayload
): Promise<string | null> {
  const explicitSessionId =
    getAttributeValue(payload.note_attributes, 'roas_radar_session_id') ??
    getAttributeValue(payload.attributes, 'roas_radar_session_id');
  const normalizedExplicitSessionId = toNullableUuid(explicitSessionId);

  if (normalizedExplicitSessionId) {
    return normalizedExplicitSessionId;
  }

  if (payload.checkout_token) {
    const checkoutMatch = await client.query<{ session_id: string }>(
      `
        SELECT e.session_id
        FROM tracking_events e
        WHERE e.shopify_checkout_token = $1
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `,
      [payload.checkout_token]
    );

    if (checkoutMatch.rowCount) {
      return checkoutMatch.rows[0].session_id;
    }
  }

  if (payload.cart_token) {
    const cartMatch = await client.query<{ session_id: string }>(
      `
        SELECT e.session_id
        FROM tracking_events e
        WHERE e.shopify_cart_token = $1
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `,
      [payload.cart_token]
    );

    if (cartMatch.rowCount) {
      return cartMatch.rows[0].session_id;
    }
  }

  return null;
}

async function upsertShopifyOrderLineItems(
  client: PoolClient,
  shopifyOrderId: string,
  lineItems: ShopifyLineItemPayload[]
): Promise<void> {
  await client.query('DELETE FROM shopify_order_line_items WHERE shopify_order_id = $1', [shopifyOrderId]);

  for (const [index, lineItem] of lineItems.entries()) {
    await client.query(
      `
        INSERT INTO shopify_order_line_items (
          shopify_order_id,
          shopify_line_item_id,
          shopify_product_id,
          shopify_variant_id,
          sku,
          title,
          variant_title,
          vendor,
          quantity,
          price,
          total_discount,
          fulfillment_status,
          requires_shipping,
          taxable,
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
          $14,
          $15::jsonb,
          now()
        )
      `,
      [
        shopifyOrderId,
        buildLineItemExternalId(shopifyOrderId, lineItem, index),
        lineItem.product_id !== undefined && lineItem.product_id !== null ? String(lineItem.product_id) : null,
        lineItem.variant_id !== undefined && lineItem.variant_id !== null ? String(lineItem.variant_id) : null,
        normalizeLineItemText(lineItem.sku),
        normalizeLineItemText(lineItem.title),
        normalizeLineItemText(lineItem.name),
        normalizeLineItemText(lineItem.vendor),
        lineItem.quantity ?? 0,
        toNumericString(lineItem.price),
        toNumericString(lineItem.total_discount),
        normalizeLineItemText(lineItem.fulfillment_status),
        lineItem.requires_shipping ?? null,
        lineItem.taxable ?? null,
        JSON.stringify(lineItem)
      ]
    );
  }
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

async function markWebhookReceiptStatus(receiptId: number, status: 'processed' | 'failed' | 'ignored'): Promise<void> {
  await query(
    `
      UPDATE shopify_webhook_receipts
      SET
        status = $2,
        processed_at = CASE WHEN $2 IN ('processed', 'ignored') THEN now() ELSE processed_at END
      WHERE id = $1
    `,
    [receiptId, status]
  );
}

async function normalizeShopifyOrder(receiptId: number, payload: ShopifyOrderPayload): Promise<void> {
  const shopifyCustomerId = payload.customer?.id ? String(payload.customer.id) : null;
  const normalizedOrderEmail = payload.email ?? payload.customer?.email ?? null;
  const orderEmailHash = hashIdentityEmail(normalizedOrderEmail);
  const shopifyOrderId = String(payload.id);

  await withTransaction(async (client) => {
    const landingSessionId = await resolveLandingSessionId(client, payload);

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
        shopifyOrderId,
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
        landingSessionId,
        payload.checkout_token ?? null,
        payload.cart_token ?? null,
        payload.source_name ?? null,
        JSON.stringify(payload)
      ]
    );

    await upsertShopifyOrderLineItems(client, shopifyOrderId, payload.line_items);

    await stitchKnownCustomerIdentity(client, {
      shopifyOrderId,
      shopifyCustomerId,
      email: normalizedOrderEmail,
      landingSessionId,
      checkoutToken: payload.checkout_token ?? null,
      cartToken: payload.cart_token ?? null
    });

    await enqueueAttributionForOrder(shopifyOrderId, 'shopify_order_upserted', client);

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
    logInfo('shopify_webhook_duplicate', {
      topic: input.topic,
      shopDomain: input.shopDomain,
      webhookId: input.webhookId
    });
    return { duplicated: true };
  }

  if (!isEligibleOnlineStoreOrder(input.payload.source_name)) {
    await markWebhookReceiptStatus(receipt.id, 'ignored');
    logInfo('shopify_webhook_ignored', {
      topic: input.topic,
      shopDomain: input.shopDomain,
      webhookId: input.webhookId,
      sourceName: input.payload.source_name ?? null
    });
    return { duplicated: false };
  }

  try {
    await normalizeShopifyOrder(receipt.id, input.payload);
    logInfo('shopify_webhook_processed', {
      topic: input.topic,
      shopDomain: input.shopDomain,
      webhookId: input.webhookId,
      duplicated: false,
      shopifyOrderId: String(input.payload.id)
    });
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
        logWarning('shopify_webhook_rejected', {
          topic: defaultTopic,
          reason: 'missing_raw_payload'
        });
        res.status(400).json({ error: 'Expected a raw Shopify webhook payload.' });
        return;
      }

      if (!verifyWebhookSignature(rawBody, signature)) {
        logWarning('shopify_webhook_rejected', {
          topic: defaultTopic,
          reason: 'invalid_signature'
        });
        res.status(401).json({ error: 'Invalid Shopify webhook signature.' });
        return;
      }

      const shopDomain = normalizeShopDomain(req.header('x-shopify-shop-domain') ?? 'unknown.myshopify.com');
      const activeShopDomain = await getActiveInstalledShopDomain();

      if (activeShopDomain && activeShopDomain !== shopDomain) {
        logWarning('shopify_webhook_rejected', {
          topic: defaultTopic,
          reason: 'shop_domain_mismatch',
          shopDomain
        });
        res.status(403).json({ error: 'Webhook shop domain does not match the connected store.' });
        return;
      }

      const payload = shopifyOrderPayloadSchema.parse(JSON.parse(rawBody.toString('utf8')));
      await persistWebhook({
        payload,
        rawBody,
        topic: req.header('x-shopify-topic') ?? defaultTopic,
        shopDomain,
        webhookId: req.header('x-shopify-webhook-id') ?? null
      });

      res.status(200).json({ ok: true });
    } catch (error) {
      logError('shopify_webhook_failed', error, {
        topic: req.header('x-shopify-topic') ?? defaultTopic,
        webhookId: req.header('x-shopify-webhook-id') ?? null
      });
      next(error);
    }
  };
}

function createAppUninstalledWebhookHandler(): RequestHandler {
  return async (req, res, next) => {
    try {
      const rawBody = req.body as Buffer;
      const signature = req.header('x-shopify-hmac-sha256') ?? undefined;

      if (!Buffer.isBuffer(rawBody)) {
        logWarning('shopify_webhook_rejected', {
          topic: 'app/uninstalled',
          reason: 'missing_raw_payload'
        });
        res.status(400).json({ error: 'Expected a raw Shopify webhook payload.' });
        return;
      }

      if (!verifyWebhookSignature(rawBody, signature)) {
        logWarning('shopify_webhook_rejected', {
          topic: 'app/uninstalled',
          reason: 'invalid_signature'
        });
        res.status(401).json({ error: 'Invalid Shopify webhook signature.' });
        return;
      }

      const shopDomain = normalizeShopDomain(req.header('x-shopify-shop-domain') ?? 'unknown.myshopify.com');
      await markInstallationUninstalled(shopDomain);
      logInfo('shopify_app_uninstalled', {
        shopDomain
      });

      res.status(200).json({ ok: true });
    } catch (error) {
      logError('shopify_webhook_failed', error, {
        topic: 'app/uninstalled'
      });
      next(error);
    }
  };
}

export function createShopifyPublicRouter(): Router {
  const router = Router();

  router.get('/install', async (req, res, next) => {
    try {
      assertShopifyAppConfig();

      const input = installRequestSchema.parse(req.query);
      const shopDomain = normalizeShopDomain(input.shop);
      const returnTo = normalizeReturnTo(input.returnTo);

      await assertSingleStoreInstallAllowed(shopDomain);

      const state = randomBytes(32).toString('hex');
      await persistOAuthState(shopDomain, state, returnTo);

      res.redirect(302, buildShopifyInstallUrl(shopDomain, state, returnTo));
    } catch (error) {
      next(error);
    }
  });

  router.get('/oauth/callback', async (req, res, next) => {
    try {
      assertShopifyAppConfig();

      const input = callbackQuerySchema.parse(req.query);
      const shopDomain = normalizeShopDomain(input.shop);

      if (!verifyShopifyOAuthHmac(req.originalUrl, input.hmac)) {
        throw new ShopifyHttpError(401, 'invalid_shopify_oauth_hmac', 'Invalid Shopify OAuth callback signature');
      }

      await assertSingleStoreInstallAllowed(shopDomain);
      const state = await consumeOAuthState(shopDomain, input.state);
      const { accessToken, scopes } = await exchangeCodeForAccessToken(shopDomain, input.code);
      const shopIdentity = await fetchShopIdentity(shopDomain, accessToken);
      const canonicalShopDomain = normalizeShopDomain(shopIdentity.myshopifyDomain);
      const webhookBaseUrl = `${getAppBaseUrl()}/webhooks/shopify`;
      const webhookSubscriptions = await provisionWebhookSubscriptions(
        canonicalShopDomain,
        accessToken,
        webhookBaseUrl
      );

      await upsertShopifyInstallation({
        shopDomain: canonicalShopDomain,
        accessToken,
        scopes,
        webhookBaseUrl,
        webhookSubscriptions,
        shopIdentity
      });

      const redirectTarget = state.return_to ?? normalizeReturnTo(env.SHOPIFY_APP_POST_INSTALL_REDIRECT_URL) ?? null;

      if (redirectTarget) {
        const redirectUrl = redirectTarget.startsWith('/')
          ? `${getAppBaseUrl()}${redirectTarget}`
          : new URL(redirectTarget).toString();
        const destination = new URL(redirectUrl);
        destination.searchParams.set('shop', canonicalShopDomain);
        destination.searchParams.set('status', 'connected');
        res.redirect(302, destination.toString());
        return;
      }

      res.status(200).json({
        ok: true,
        shopDomain: canonicalShopDomain,
        scopes,
        webhooksRegistered: webhookSubscriptions
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}

export function createShopifyWebhookRouter(): Router {
  const router = Router();

  router.post('/orders-create', createOrderWebhookHandler('orders/create'));
  router.post('/orders-paid', createOrderWebhookHandler('orders/paid'));
  router.post('/app-uninstalled', createAppUninstalledWebhookHandler());

  return router;
}

export function createShopifyAdminRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAdmin);

  router.get('/connection', async (_req, res, next) => {
    try {
      const installation = await getShopifyInstallationSummary();

      if (!installation) {
        res.json({
          connected: false,
          shopDomain: null,
          installUrl: null,
          reconnectUrl: null
        });
        return;
      }

      let reconnectUrl: string | null = null;

      if (env.SHOPIFY_APP_API_KEY && env.SHOPIFY_APP_API_SECRET && env.SHOPIFY_APP_API_VERSION && env.SHOPIFY_APP_BASE_URL) {
        const params = new URLSearchParams({ shop: installation.shop_domain });
        reconnectUrl = `${getAppBaseUrl()}/shopify/install?${params.toString()}`;
      }

      res.json({
        connected: installation.status === 'active',
        shopDomain: installation.shop_domain,
        status: installation.status,
        installedAt: installation.installed_at.toISOString(),
        reconnectedAt: installation.reconnected_at?.toISOString() ?? null,
        uninstalledAt: installation.uninstalled_at?.toISOString() ?? null,
        scopes: installation.scopes,
        webhookBaseUrl: installation.webhook_base_url,
        webhookSubscriptions: installation.webhook_subscriptions,
        shop: {
          name: installation.shop_name,
          email: installation.shop_email,
          currency: installation.shop_currency
        },
        reconnectUrl
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/webhooks/sync', async (_req, res, next) => {
    try {
      assertShopifyAppConfig();

      const installation = await getShopifyInstallationSummary();

      if (!installation || installation.status !== 'active') {
        throw new ShopifyHttpError(404, 'shopify_installation_not_found', 'No active Shopify installation was found');
      }

      const accessToken = await getActiveShopifyAccessToken(installation.shop_domain);
      const webhookBaseUrl = `${getAppBaseUrl()}/webhooks/shopify`;
      const webhookSubscriptions = await provisionWebhookSubscriptions(
        installation.shop_domain,
        accessToken,
        webhookBaseUrl
      );

      await query(
        `
          UPDATE shopify_app_installations
          SET
            webhook_base_url = $2,
            webhook_subscriptions = $3::jsonb,
            updated_at = now()
          WHERE shop_domain = $1
        `,
        [installation.shop_domain, webhookBaseUrl, JSON.stringify(webhookSubscriptions)]
      );

      res.status(200).json({
        ok: true,
        shopDomain: installation.shop_domain,
        webhookSubscriptions
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}

export const __shopifyTestUtils = {
  normalizeShopDomain,
  createOAuthHmacMessage,
  verifyShopifyOAuthHmac,
  buildShopifyInstallUrl,
  verifyWebhookSignature,
  isEligibleOnlineStoreOrder,
  buildLineItemExternalId
};
