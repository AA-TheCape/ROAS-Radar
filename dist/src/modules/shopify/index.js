"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.__shopifyTestUtils = void 0;
exports.createShopifyPublicRouter = createShopifyPublicRouter;
exports.createShopifyWebhookRouter = createShopifyWebhookRouter;
exports.createShopifyAdminRouter = createShopifyAdminRouter;
const node_crypto_1 = require("node:crypto");
const express_1 = require("express");
const zod_1 = require("zod");
const index_js_1 = require("../../../packages/attribution-schema/index.js");
const env_js_1 = require("../../config/env.js");
const pool_js_1 = require("../../db/pool.js");
const index_js_2 = require("../../observability/index.js");
const index_js_3 = require("../attribution/index.js");
const index_js_4 = require("../auth/index.js");
const index_js_5 = require("../identity/index.js");
const index_js_6 = require("../marketing-dimensions/index.js");
const index_js_7 = require("../settings/index.js");
const writeback_js_1 = require("./writeback.js");
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
];
const strictEmailSchema = zod_1.z.string().email();
function sanitizeNullableEmail(value) {
    if (value == null) {
        return value;
    }
    if (typeof value !== 'string') {
        return null;
    }
    const normalized = value.trim().toLowerCase();
    if (!normalized) {
        return null;
    }
    return strictEmailSchema.safeParse(normalized).success ? normalized : null;
}
const shopifyEmailSchema = zod_1.z.preprocess(sanitizeNullableEmail, zod_1.z.string().nullable().optional());
const shopifyAttributeSchema = zod_1.z.object({
    name: zod_1.z.string().optional(),
    value: zod_1.z.union([zod_1.z.string(), zod_1.z.number(), zod_1.z.boolean()]).nullable().optional()
});
const shopifyCustomerSchema = zod_1.z
    .object({
    id: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).optional(),
    email: shopifyEmailSchema,
    phone: zod_1.z.string().nullable().optional()
})
    .nullable()
    .optional();
const shopifyLineItemSchema = zod_1.z.object({
    id: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).optional(),
    product_id: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).nullable().optional(),
    variant_id: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).nullable().optional(),
    sku: zod_1.z.string().nullable().optional(),
    title: zod_1.z.string().nullable().optional(),
    name: zod_1.z.string().nullable().optional(),
    vendor: zod_1.z.string().nullable().optional(),
    quantity: zod_1.z.coerce.number().int().optional(),
    price: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).optional(),
    total_discount: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).optional(),
    fulfillment_status: zod_1.z.string().nullable().optional(),
    requires_shipping: zod_1.z.boolean().nullable().optional(),
    taxable: zod_1.z.boolean().nullable().optional()
});
const shopifyOrderPayloadSchema = zod_1.z.object({
    id: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]),
    order_number: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).optional(),
    customer: shopifyCustomerSchema,
    email: shopifyEmailSchema,
    currency: zod_1.z.string().min(1).optional(),
    subtotal_price: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).optional(),
    total_price: zod_1.z.union([zod_1.z.string(), zod_1.z.number()]).optional(),
    financial_status: zod_1.z.string().nullable().optional(),
    fulfillment_status: zod_1.z.string().nullable().optional(),
    processed_at: zod_1.z.string().datetime({ offset: true }).nullable().optional(),
    created_at: zod_1.z.string().datetime({ offset: true }).nullable().optional(),
    updated_at: zod_1.z.string().datetime({ offset: true }).nullable().optional(),
    checkout_token: zod_1.z.string().nullable().optional(),
    cart_token: zod_1.z.string().nullable().optional(),
    landing_site: zod_1.z.string().nullable().optional(),
    referring_site: zod_1.z.string().nullable().optional(),
    source_name: zod_1.z.string().nullable().optional(),
    line_items: zod_1.z.array(shopifyLineItemSchema).default([]),
    note_attributes: zod_1.z.array(shopifyAttributeSchema).optional(),
    attributes: zod_1.z.array(shopifyAttributeSchema).optional()
});
const installRequestSchema = zod_1.z.object({
    shop: zod_1.z.string().min(1),
    returnTo: zod_1.z.string().optional()
});
const callbackQuerySchema = zod_1.z.object({
    shop: zod_1.z.string().min(1),
    code: zod_1.z.string().min(1),
    hmac: zod_1.z.string().min(1),
    state: zod_1.z.string().min(1)
});
const shopifyBackfillRequestSchema = zod_1.z
    .object({
    startDate: zod_1.z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    endDate: zod_1.z.string().regex(/^\d{4}-\d{2}-\d{2}$/)
})
    .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
        ctx.addIssue({
            code: zod_1.z.ZodIssueCode.custom,
            message: 'startDate must be on or before endDate',
            path: ['startDate']
        });
    }
});
class ShopifyHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'ShopifyHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
function getShopifySharedSecret() {
    return env_js_1.env.SHOPIFY_WEBHOOK_SECRET || env_js_1.env.SHOPIFY_APP_API_SECRET;
}
function normalizeShopDomain(rawShop) {
    const normalized = rawShop.trim().toLowerCase();
    if (!/^[a-z0-9][a-z0-9-]*\.myshopify\.com$/.test(normalized)) {
        throw new ShopifyHttpError(400, 'invalid_shop_domain', 'shop must be a valid *.myshopify.com domain');
    }
    return normalized;
}
function normalizeReturnTo(rawReturnTo) {
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
    }
    catch {
        throw new ShopifyHttpError(400, 'invalid_return_to', 'returnTo must be an absolute http(s) URL or a root-relative path');
    }
}
function assertShopifyAppConfig() {
    const missing = [
        ['SHOPIFY_APP_API_KEY', env_js_1.env.SHOPIFY_APP_API_KEY],
        ['SHOPIFY_APP_API_SECRET', env_js_1.env.SHOPIFY_APP_API_SECRET],
        ['SHOPIFY_APP_API_VERSION', env_js_1.env.SHOPIFY_APP_API_VERSION],
        ['SHOPIFY_APP_BASE_URL', env_js_1.env.SHOPIFY_APP_BASE_URL],
        ['SHOPIFY_APP_ENCRYPTION_KEY', env_js_1.env.SHOPIFY_APP_ENCRYPTION_KEY]
    ]
        .filter(([, value]) => !value)
        .map(([key]) => key);
    if (missing.length > 0) {
        throw new ShopifyHttpError(500, 'shopify_app_config_missing', `Missing Shopify app configuration: ${missing.join(', ')}`);
    }
}
function getAppBaseUrl() {
    const url = new URL(env_js_1.env.SHOPIFY_APP_BASE_URL);
    return url.toString().replace(/\/$/, '');
}
function buildOAuthCallbackUrl() {
    return `${getAppBaseUrl()}/shopify/oauth/callback`;
}
function createOAuthStateDigest(state) {
    return (0, node_crypto_1.createHash)('sha256').update(state).digest('hex');
}
function verifyWebhookSignature(rawBody, signature) {
    const secret = getShopifySharedSecret();
    if (!secret || !signature) {
        return false;
    }
    const digest = (0, node_crypto_1.createHmac)('sha256', secret).update(rawBody).digest('base64');
    if (digest.length !== signature.length) {
        return false;
    }
    return (0, node_crypto_1.timingSafeEqual)(Buffer.from(digest), Buffer.from(signature));
}
function createOAuthHmacMessage(originalUrl) {
    const queryIndex = originalUrl.indexOf('?');
    if (queryIndex === -1) {
        return '';
    }
    const params = new URLSearchParams(originalUrl.slice(queryIndex + 1));
    const pairs = [];
    for (const [key, value] of params.entries()) {
        if (key === 'hmac' || key === 'signature') {
            continue;
        }
        pairs.push(`${key}=${value}`);
    }
    return pairs.sort().join('&');
}
function verifyShopifyOAuthHmac(originalUrl, providedHmac) {
    const secret = env_js_1.env.SHOPIFY_APP_API_SECRET;
    if (!secret || !providedHmac) {
        return false;
    }
    const digest = (0, node_crypto_1.createHmac)('sha256', secret).update(createOAuthHmacMessage(originalUrl)).digest('hex');
    if (digest.length !== providedHmac.length) {
        return false;
    }
    return (0, node_crypto_1.timingSafeEqual)(Buffer.from(digest), Buffer.from(providedHmac));
}
function buildShopifyInstallUrl(shopDomain, state, returnTo) {
    assertShopifyAppConfig();
    const url = new URL(`https://${shopDomain}/admin/oauth/authorize`);
    url.searchParams.set('client_id', env_js_1.env.SHOPIFY_APP_API_KEY);
    url.searchParams.set('scope', env_js_1.env.SHOPIFY_APP_SCOPES.join(','));
    url.searchParams.set('redirect_uri', buildOAuthCallbackUrl());
    url.searchParams.set('state', state);
    if (returnTo) {
        url.searchParams.set('return_to', returnTo);
    }
    return url.toString();
}
async function assertSingleStoreInstallAllowed(shopDomain) {
    const result = await (0, pool_js_1.query)(`
      SELECT shop_domain, status
      FROM shopify_app_installations
      WHERE shop_domain <> $1
        AND status <> 'uninstalled'
      ORDER BY updated_at DESC
      LIMIT 1
    `, [shopDomain]);
    if (result.rowCount) {
        throw new ShopifyHttpError(409, 'shopify_store_already_connected', `A different Shopify store is already connected for this MVP environment: ${result.rows[0].shop_domain}`);
    }
}
async function persistOAuthState(shopDomain, state, returnTo) {
    await (0, pool_js_1.query)(`
      INSERT INTO shopify_oauth_states (
        shop_domain,
        state_digest,
        return_to,
        expires_at
      )
      VALUES ($1, $2, $3, now() + ($4 || ' minutes')::interval)
    `, [shopDomain, createOAuthStateDigest(state), returnTo, String(OAUTH_STATE_TTL_MINUTES)]);
}
async function consumeOAuthState(shopDomain, state) {
    return (0, pool_js_1.withTransaction)(async (client) => {
        const stateResult = await client.query(`
        SELECT id, return_to
        FROM shopify_oauth_states
        WHERE shop_domain = $1
          AND state_digest = $2
          AND consumed_at IS NULL
          AND expires_at > now()
        ORDER BY created_at DESC
        LIMIT 1
        FOR UPDATE
      `, [shopDomain, createOAuthStateDigest(state)]);
        if (!stateResult.rowCount) {
            throw new ShopifyHttpError(400, 'invalid_oauth_state', 'OAuth state is invalid or expired');
        }
        await client.query(`
        UPDATE shopify_oauth_states
        SET consumed_at = now()
        WHERE id = $1
      `, [stateResult.rows[0].id]);
        return stateResult.rows[0];
    });
}
async function exchangeCodeForAccessToken(shopDomain, code) {
    const response = await fetch(`https://${shopDomain}/admin/oauth/access_token`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            accept: 'application/json'
        },
        body: JSON.stringify({
            client_id: env_js_1.env.SHOPIFY_APP_API_KEY,
            client_secret: env_js_1.env.SHOPIFY_APP_API_SECRET,
            code
        })
    });
    const payload = (await response.json());
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
async function callShopifyAdminGraphql(shopDomain, accessToken, graphqlQuery, variables) {
    const response = await fetch(`https://${shopDomain}/admin/api/${env_js_1.env.SHOPIFY_APP_API_VERSION}/graphql.json`, {
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
    const payload = (await response.json());
    if (!response.ok || payload.errors?.length || !payload.data) {
        throw new ShopifyHttpError(502, 'shopify_admin_api_failed', 'Shopify Admin API request failed', payload);
    }
    return payload.data;
}
async function callShopifyAdminRest(shopDomain, accessToken, path, searchParams) {
    const url = new URL(`https://${shopDomain}/admin/api/${env_js_1.env.SHOPIFY_APP_API_VERSION}/${path.replace(/^\//, '')}`);
    if (searchParams) {
        url.search = searchParams.toString();
    }
    const response = await fetch(url, {
        method: 'GET',
        headers: {
            accept: 'application/json',
            'x-shopify-access-token': accessToken
        }
    });
    const payload = (await response.json());
    if (!response.ok || ('errors' in payload && Array.isArray(payload.errors) && payload.errors.length > 0)) {
        throw new ShopifyHttpError(502, 'shopify_admin_api_failed', 'Shopify Admin API request failed', payload);
    }
    return {
        data: payload,
        linkHeader: response.headers.get('link')
    };
}
function parseDateOnly(dateString) {
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateString);
    if (!match) {
        throw new ShopifyHttpError(400, 'invalid_date', 'Date must use YYYY-MM-DD format');
    }
    return {
        year: Number(match[1]),
        month: Number(match[2]),
        day: Number(match[3])
    };
}
function getTimeZoneLocalParts(date, timeZone) {
    const formatter = new Intl.DateTimeFormat('en-US', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23'
    });
    const parts = formatter.formatToParts(date);
    const get = (type) => Number(parts.find((part) => part.type === type)?.value ?? '0');
    return {
        year: get('year'),
        month: get('month'),
        day: get('day'),
        hour: get('hour'),
        minute: get('minute'),
        second: get('second')
    };
}
function convertLocalDateTimeInTimeZoneToUtc(dateString, timeZone, kind) {
    const { year, month, day } = parseDateOnly(dateString);
    const target = {
        year,
        month,
        day,
        hour: kind === 'start' ? 0 : 23,
        minute: kind === 'start' ? 0 : 59,
        second: kind === 'start' ? 0 : 59
    };
    let utcMillis = Date.UTC(target.year, target.month - 1, target.day, target.hour, target.minute, target.second);
    for (let attempt = 0; attempt < 4; attempt += 1) {
        const actual = getTimeZoneLocalParts(new Date(utcMillis), timeZone);
        const actualMillis = Date.UTC(actual.year, actual.month - 1, actual.day, actual.hour, actual.minute, actual.second);
        const targetMillis = Date.UTC(target.year, target.month - 1, target.day, target.hour, target.minute, target.second);
        const deltaMs = actualMillis - targetMillis;
        if (deltaMs === 0) {
            break;
        }
        utcMillis -= deltaMs;
    }
    if (kind === 'end') {
        utcMillis += 999;
    }
    return new Date(utcMillis).toISOString();
}
function extractShopifyNextPageInfo(linkHeader) {
    if (!linkHeader) {
        return null;
    }
    const nextLink = linkHeader
        .split(',')
        .map((entry) => entry.trim())
        .find((entry) => entry.includes('rel="next"'));
    if (!nextLink) {
        return null;
    }
    const match = nextLink.match(/<([^>]+)>/);
    if (!match) {
        return null;
    }
    const url = new URL(match[1]);
    return url.searchParams.get('page_info');
}
async function backfillShopifyOrders(shopDomain, accessToken, reportingTimezone, startDate, endDate) {
    let pageInfo = null;
    let importedOrders = 0;
    let processedOrders = 0;
    let duplicatedOrders = 0;
    do {
        const searchParams = new URLSearchParams();
        searchParams.set('limit', '250');
        if (pageInfo) {
            searchParams.set('page_info', pageInfo);
        }
        else {
            searchParams.set('status', 'any');
            searchParams.set('processed_at_min', convertLocalDateTimeInTimeZoneToUtc(startDate, reportingTimezone, 'start'));
            searchParams.set('processed_at_max', convertLocalDateTimeInTimeZoneToUtc(endDate, reportingTimezone, 'end'));
            searchParams.set('order', 'processed_at asc');
        }
        const { data, linkHeader } = await callShopifyAdminRest(shopDomain, accessToken, 'orders.json', searchParams);
        const rawOrders = Array.isArray(data.orders) ? data.orders : [];
        for (const rawOrder of rawOrders) {
            const payload = shopifyOrderPayloadSchema.parse(rawOrder);
            const persisted = await persistWebhook({
                payload,
                rawPayload: rawOrder,
                rawBody: Buffer.from(JSON.stringify(rawOrder)),
                shopDomain,
                topic: 'orders/backfill',
                webhookId: null
            });
            importedOrders += 1;
            if (persisted.duplicated) {
                duplicatedOrders += 1;
            }
            else {
                processedOrders += 1;
            }
        }
        pageInfo = extractShopifyNextPageInfo(linkHeader);
    } while (pageInfo);
    return {
        importedOrders,
        processedOrders,
        duplicatedOrders
    };
}
async function recoverShopifyAttributionHints(reportingTimezone, startDate, endDate) {
    const processedAtMin = convertLocalDateTimeInTimeZoneToUtc(startDate, reportingTimezone, 'start');
    const processedAtMax = convertLocalDateTimeInTimeZoneToUtc(endDate, reportingTimezone, 'end');
    const result = await (0, pool_js_1.query)(`
      SELECT
        o.shopify_order_id,
        o.shopify_customer_id,
        o.email,
        o.landing_session_id::text AS landing_session_id,
        o.raw_payload
      FROM shopify_orders o
      LEFT JOIN attribution_order_credits c
        ON c.shopify_order_id = o.shopify_order_id
       AND c.attribution_model = 'last_touch'
      WHERE COALESCE(o.source_name, '') = 'web'
        AND o.processed_at >= $1::timestamptz
        AND o.processed_at <= $2::timestamptz
        AND (
          c.shopify_order_id IS NULL
          OR (
            c.attributed_source IS NULL
            AND c.attributed_medium IS NULL
            AND c.attributed_campaign IS NULL
          )
        )
      ORDER BY o.processed_at ASC, o.shopify_order_id ASC
    `, [processedAtMin, processedAtMax]);
    let rescannedOrders = 0;
    let relinkedOrders = 0;
    let requeuedOrders = 0;
    let shopifyHintAttributedOrders = 0;
    for (const row of result.rows) {
        const parsedPayload = shopifyOrderPayloadSchema.safeParse(row.raw_payload);
        if (!parsedPayload.success) {
            (0, index_js_2.logWarning)('shopify_attribution_hint_recovery_skipped', {
                shopifyOrderId: row.shopify_order_id,
                reason: 'invalid_raw_payload',
                issues: parsedPayload.error.issues
            });
            continue;
        }
        rescannedOrders += 1;
        await (0, pool_js_1.withTransaction)(async (client) => {
            const resolvedLandingSessionId = await resolveLandingSessionId(client, parsedPayload.data);
            const shouldUpdateLandingSessionId = resolvedLandingSessionId !== null && resolvedLandingSessionId !== row.landing_session_id;
            if (shouldUpdateLandingSessionId) {
                await client.query(`
            UPDATE shopify_orders
            SET
              landing_session_id = $2::uuid,
              ingested_at = now()
            WHERE shopify_order_id = $1
          `, [row.shopify_order_id, resolvedLandingSessionId]);
                relinkedOrders += 1;
            }
            await (0, index_js_5.stitchKnownCustomerIdentity)(client, {
                shopifyOrderId: row.shopify_order_id,
                shopifyCustomerId: row.shopify_customer_id,
                email: row.email,
                landingSessionId: resolvedLandingSessionId ?? row.landing_session_id,
                checkoutToken: parsedPayload.data.checkout_token ?? null,
                cartToken: parsedPayload.data.cart_token ?? null
            });
            const shopifyHintAttribution = resolvedLandingSessionId === null ? extractShopifyHintAttribution(parsedPayload.data) : null;
            if (shopifyHintAttribution) {
                await (0, index_js_3.applySyntheticAttributionForOrder)(row.shopify_order_id, {
                    ...shopifyHintAttribution,
                    attributionReason: 'shopify_hint_derived'
                }, client);
                shopifyHintAttributedOrders += 1;
                return;
            }
            await (0, index_js_3.enqueueAttributionForOrder)(row.shopify_order_id, 'shopify_attribution_hint_recovery', client);
            requeuedOrders += 1;
        });
    }
    return {
        rescannedOrders,
        relinkedOrders,
        requeuedOrders,
        shopifyHintAttributedOrders
    };
}
async function fetchShopIdentity(shopDomain, accessToken) {
    const data = await callShopifyAdminGraphql(shopDomain, accessToken, `
      query ConnectedShop {
        shop {
          name
          email
          currencyCode
          myshopifyDomain
        }
      }
    `);
    return {
        myshopifyDomain: data.shop.myshopifyDomain,
        name: data.shop.name,
        email: data.shop.email,
        currencyCode: data.shop.currencyCode,
        raw: data.shop
    };
}
async function provisionWebhookSubscriptions(shopDomain, accessToken, webhookBaseUrl) {
    const subscriptions = [];
    for (const subscription of SHOPIFY_WEBHOOK_TOPICS) {
        const callbackUrl = `${webhookBaseUrl}${subscription.deliveryPath}`;
        const data = await callShopifyAdminGraphql(shopDomain, accessToken, `
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
      `, {
            topic: subscription.topic,
            callbackUrl
        });
        if (data.webhookSubscriptionCreate.userErrors.length > 0 || !data.webhookSubscriptionCreate.webhookSubscription) {
            throw new ShopifyHttpError(502, 'shopify_webhook_registration_failed', `Failed to register Shopify webhook for ${subscription.topic}`, data.webhookSubscriptionCreate.userErrors);
        }
        subscriptions.push({
            topic: data.webhookSubscriptionCreate.webhookSubscription.topic,
            id: data.webhookSubscriptionCreate.webhookSubscription.id,
            callbackUrl: data.webhookSubscriptionCreate.webhookSubscription.endpoint?.callbackUrl ?? null
        });
    }
    return subscriptions;
}
async function upsertShopifyInstallation(params) {
    await (0, pool_js_1.query)(`
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
    `, [
        params.shopDomain,
        params.accessToken,
        env_js_1.env.SHOPIFY_APP_ENCRYPTION_KEY,
        params.scopes,
        params.webhookBaseUrl,
        JSON.stringify(params.webhookSubscriptions),
        params.shopIdentity.name,
        params.shopIdentity.email,
        params.shopIdentity.currencyCode,
        JSON.stringify(params.shopIdentity.raw)
    ]);
}
async function getShopifyInstallationSummary() {
    const result = await (0, pool_js_1.query)(`
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
    `);
    return result.rows[0] ?? null;
}
async function getActiveShopifyAccessToken(shopDomain) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        pgp_sym_decrypt(access_token_encrypted, $2) AS access_token
      FROM shopify_app_installations
      WHERE shop_domain = $1
        AND status = 'active'
      LIMIT 1
    `, [shopDomain, env_js_1.env.SHOPIFY_APP_ENCRYPTION_KEY]);
    if (!result.rowCount) {
        throw new ShopifyHttpError(404, 'shopify_installation_not_found', 'No active Shopify installation was found');
    }
    return result.rows[0].access_token;
}
async function getActiveInstalledShopDomain() {
    const result = await (0, pool_js_1.query)(`
      SELECT shop_domain
      FROM shopify_app_installations
      WHERE status = 'active'
      ORDER BY updated_at DESC
      LIMIT 1
    `);
    return result.rows[0]?.shop_domain ?? null;
}
async function markInstallationUninstalled(shopDomain) {
    await (0, pool_js_1.query)(`
      UPDATE shopify_app_installations
      SET
        status = 'uninstalled',
        uninstalled_at = now(),
        updated_at = now()
      WHERE shop_domain = $1
    `, [shopDomain]);
}
function getAttributeValue(attributes, key) {
    const match = attributes?.find((attribute) => attribute.name === key);
    if (match?.value === undefined || match.value === null) {
        return null;
    }
    const normalized = String(match.value).trim();
    return normalized ? normalized : null;
}
function getAttributeValueFromKeys(attributes, keys) {
    for (const key of keys) {
        const value = getAttributeValue(attributes, key);
        if (value) {
            return value;
        }
    }
    return null;
}
function toNumericString(value) {
    if (value === undefined) {
        return '0';
    }
    return String(value);
}
function toNullableUuid(value) {
    if (!value) {
        return null;
    }
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
        ? value
        : null;
}
function normalizeNullableString(value) {
    return (0, index_js_1.normalizeAttributionString)(value);
}
function isEligibleOnlineStoreOrder(sourceName) {
    const normalizedSource = normalizeNullableString(sourceName)?.toLowerCase();
    return normalizedSource === 'web';
}
function buildLineItemExternalId(orderId, lineItem, index) {
    if (lineItem.id !== undefined && lineItem.id !== null) {
        return String(lineItem.id);
    }
    return `${orderId}:line:${index + 1}`;
}
function normalizeLineItemText(value) {
    return normalizeNullableString(value);
}
function hasAttributionDimensions(value) {
    return Boolean(value.source ||
        value.medium ||
        value.campaign ||
        value.content ||
        value.term ||
        value.clickIdType ||
        value.clickIdValue);
}
function extractShopifyHintAttribution(payload) {
    const noteAttributes = payload.note_attributes;
    const legacyAttributes = payload.attributes;
    const rawDimensions = {
        source: getAttributeValueFromKeys(noteAttributes, ['utm_source', 'roas_radar_utm_source']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_source', 'roas_radar_utm_source']),
        medium: getAttributeValueFromKeys(noteAttributes, ['utm_medium', 'roas_radar_utm_medium']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_medium', 'roas_radar_utm_medium']),
        campaign: getAttributeValueFromKeys(noteAttributes, ['utm_campaign', 'roas_radar_utm_campaign']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_campaign', 'roas_radar_utm_campaign']),
        content: getAttributeValueFromKeys(noteAttributes, ['utm_content', 'roas_radar_utm_content']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_content', 'roas_radar_utm_content']),
        term: getAttributeValueFromKeys(noteAttributes, ['utm_term', 'roas_radar_utm_term']) ??
            getAttributeValueFromKeys(legacyAttributes, ['utm_term', 'roas_radar_utm_term']),
        gclid: getAttributeValueFromKeys(noteAttributes, ['gclid', 'roas_radar_gclid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['gclid', 'roas_radar_gclid']),
        gbraid: getAttributeValueFromKeys(noteAttributes, ['gbraid', 'roas_radar_gbraid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['gbraid', 'roas_radar_gbraid']),
        wbraid: getAttributeValueFromKeys(noteAttributes, ['wbraid', 'roas_radar_wbraid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['wbraid', 'roas_radar_wbraid']),
        fbclid: getAttributeValueFromKeys(noteAttributes, ['fbclid', 'roas_radar_fbclid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['fbclid', 'roas_radar_fbclid']),
        ttclid: getAttributeValueFromKeys(noteAttributes, ['ttclid', 'roas_radar_ttclid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['ttclid', 'roas_radar_ttclid']),
        msclkid: getAttributeValueFromKeys(noteAttributes, ['msclkid', 'roas_radar_msclkid']) ??
            getAttributeValueFromKeys(legacyAttributes, ['msclkid', 'roas_radar_msclkid'])
    };
    const hintCandidates = [
        payload.landing_site,
        getAttributeValueFromKeys(noteAttributes, ['landing_url', 'page_url', 'roas_radar_landing_path', 'landing_site']),
        getAttributeValueFromKeys(legacyAttributes, ['landing_url', 'page_url', 'roas_radar_landing_path', 'landing_site'])
    ].filter((value) => Boolean(value));
    for (const candidate of hintCandidates) {
        try {
            const url = new URL((0, index_js_1.normalizeAttributionUrl)(candidate, 'https://shopify-hint.local') ?? candidate);
            rawDimensions.source ??= normalizeNullableString(url.searchParams.get('utm_source'));
            rawDimensions.medium ??= normalizeNullableString(url.searchParams.get('utm_medium'));
            rawDimensions.campaign ??= normalizeNullableString(url.searchParams.get('utm_campaign'));
            rawDimensions.content ??= normalizeNullableString(url.searchParams.get('utm_content'));
            rawDimensions.term ??= normalizeNullableString(url.searchParams.get('utm_term'));
            rawDimensions.gclid ??= normalizeNullableString(url.searchParams.get('gclid'));
            rawDimensions.gbraid ??= normalizeNullableString(url.searchParams.get('gbraid'));
            rawDimensions.wbraid ??= normalizeNullableString(url.searchParams.get('wbraid'));
            rawDimensions.fbclid ??= normalizeNullableString(url.searchParams.get('fbclid'));
            rawDimensions.ttclid ??= normalizeNullableString(url.searchParams.get('ttclid'));
            rawDimensions.msclkid ??= normalizeNullableString(url.searchParams.get('msclkid'));
        }
        catch { }
    }
    const canonicalDimensions = (0, index_js_6.buildCanonicalTouchpointDimensions)({
        source: rawDimensions.source,
        medium: rawDimensions.medium,
        campaign: rawDimensions.campaign,
        content: rawDimensions.content,
        term: rawDimensions.term,
        gclid: rawDimensions.gclid,
        gbraid: rawDimensions.gbraid,
        wbraid: rawDimensions.wbraid,
        fbclid: rawDimensions.fbclid,
        ttclid: rawDimensions.ttclid,
        msclkid: rawDimensions.msclkid
    });
    if (!hasAttributionDimensions(canonicalDimensions)) {
        return null;
    }
    return {
        ...canonicalDimensions,
        confidenceScore: canonicalDimensions.clickIdValue ? 0.55 : 0.4
    };
}
async function resolveLandingSessionId(client, payload) {
    const sessionExists = async (sessionId) => {
        const existingSession = await client.query(`
        SELECT s.id::text AS id
        FROM tracking_sessions s
        WHERE s.id = $1::uuid
        LIMIT 1
      `, [sessionId]);
        return (existingSession.rowCount ?? 0) > 0;
    };
    const explicitSessionId = getAttributeValue(payload.note_attributes, 'roas_radar_session_id') ??
        getAttributeValue(payload.attributes, 'roas_radar_session_id');
    const normalizedExplicitSessionId = toNullableUuid(explicitSessionId);
    if (normalizedExplicitSessionId && (await sessionExists(normalizedExplicitSessionId))) {
        return normalizedExplicitSessionId;
    }
    if (payload.checkout_token) {
        const checkoutMatch = await client.query(`
        SELECT e.session_id
        FROM tracking_events e
        INNER JOIN tracking_sessions s ON s.id = e.session_id
        WHERE e.shopify_checkout_token = $1
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `, [payload.checkout_token]);
        if (checkoutMatch.rowCount) {
            return checkoutMatch.rows[0].session_id;
        }
    }
    if (payload.cart_token) {
        const cartMatch = await client.query(`
        SELECT e.session_id
        FROM tracking_events e
        INNER JOIN tracking_sessions s ON s.id = e.session_id
        WHERE e.shopify_cart_token = $1
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `, [payload.cart_token]);
        if (cartMatch.rowCount) {
            return cartMatch.rows[0].session_id;
        }
    }
    return null;
}
async function upsertShopifyOrderLineItems(client, shopifyOrderId, lineItems) {
    await client.query('DELETE FROM shopify_order_line_items WHERE shopify_order_id = $1', [shopifyOrderId]);
    for (const [index, lineItem] of lineItems.entries()) {
        await client.query(`
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
      `, [
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
        ]);
    }
}
async function createOrReuseWebhookReceipt(topic, shopDomain, webhookId, payloadHash, rawPayload) {
    const insertResult = await (0, pool_js_1.query)(`
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
    `, [topic, shopDomain, webhookId, payloadHash, JSON.stringify(rawPayload)]);
    if (insertResult.rowCount) {
        return insertResult.rows[0];
    }
    const existingResult = await (0, pool_js_1.query)(`
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
    `, [webhookId, topic, shopDomain, payloadHash]);
    if (!existingResult.rowCount) {
        throw new Error('Failed to create Shopify webhook receipt.');
    }
    return existingResult.rows[0];
}
async function markWebhookReceiptStatus(receiptId, status) {
    await (0, pool_js_1.query)(`
      UPDATE shopify_webhook_receipts
      SET
        status = $2,
        processed_at = CASE WHEN $2 IN ('processed', 'ignored') THEN now() ELSE processed_at END
      WHERE id = $1
    `, [receiptId, status]);
}
async function normalizeShopifyOrder(receiptId, payload, rawPayload) {
    const shopifyCustomerId = payload.customer?.id ? String(payload.customer.id) : null;
    const normalizedOrderEmail = payload.email ?? payload.customer?.email ?? null;
    const orderEmailHash = (0, index_js_5.hashIdentityEmail)(normalizedOrderEmail);
    const shopifyOrderId = String(payload.id);
    await (0, pool_js_1.withTransaction)(async (client) => {
        const landingSessionId = await resolveLandingSessionId(client, payload);
        if (shopifyCustomerId) {
            await client.query(`
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
        `, [shopifyCustomerId, normalizedOrderEmail, orderEmailHash, payload.customer?.phone ?? null]);
        }
        await client.query(`
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
      `, [
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
            JSON.stringify(rawPayload)
        ]);
        await upsertShopifyOrderLineItems(client, shopifyOrderId, payload.line_items);
        await (0, index_js_5.stitchKnownCustomerIdentity)(client, {
            shopifyOrderId,
            shopifyCustomerId,
            email: normalizedOrderEmail,
            landingSessionId,
            checkoutToken: payload.checkout_token ?? null,
            cartToken: payload.cart_token ?? null
        });
        await (0, index_js_3.enqueueAttributionForOrder)(shopifyOrderId, 'shopify_order_upserted', client);
        await (0, writeback_js_1.enqueueShopifyOrderWriteback)(shopifyOrderId, 'shopify_order_upserted', client);
        await client.query(`
        UPDATE shopify_webhook_receipts
        SET
          status = 'processed',
          processed_at = now()
        WHERE id = $1
      `, [receiptId]);
    });
}
async function persistWebhook(input) {
    const payloadHash = (0, node_crypto_1.createHash)('sha256').update(input.rawBody).digest('hex');
    const receipt = await createOrReuseWebhookReceipt(input.topic, input.shopDomain, input.webhookId, payloadHash, input.rawPayload);
    if (receipt.status === 'processed') {
        if (input.topic === 'orders/backfill') {
            await normalizeShopifyOrder(receipt.id, input.payload, input.rawPayload);
            (0, index_js_2.logInfo)('shopify_webhook_reprocessed_from_backfill', {
                topic: input.topic,
                shopDomain: input.shopDomain,
                webhookId: input.webhookId,
                shopifyOrderId: String(input.payload.id)
            });
            return { duplicated: true };
        }
        (0, index_js_2.logInfo)('shopify_webhook_duplicate', {
            topic: input.topic,
            shopDomain: input.shopDomain,
            webhookId: input.webhookId
        });
        return { duplicated: true };
    }
    if (!isEligibleOnlineStoreOrder(input.payload.source_name)) {
        await markWebhookReceiptStatus(receipt.id, 'ignored');
        (0, index_js_2.logInfo)('shopify_webhook_ignored', {
            topic: input.topic,
            shopDomain: input.shopDomain,
            webhookId: input.webhookId,
            sourceName: input.payload.source_name ?? null
        });
        return { duplicated: false };
    }
    try {
        await normalizeShopifyOrder(receipt.id, input.payload, input.rawPayload);
        (0, index_js_2.logInfo)('shopify_webhook_processed', {
            topic: input.topic,
            shopDomain: input.shopDomain,
            webhookId: input.webhookId,
            duplicated: false,
            shopifyOrderId: String(input.payload.id)
        });
        return { duplicated: false };
    }
    catch (error) {
        await markWebhookReceiptStatus(receipt.id, 'failed');
        throw error;
    }
}
function createOrderWebhookHandler(defaultTopic) {
    return async (req, res, next) => {
        try {
            const rawBody = req.body;
            const signature = req.header('x-shopify-hmac-sha256') ?? undefined;
            if (!Buffer.isBuffer(rawBody)) {
                (0, index_js_2.logWarning)('shopify_webhook_rejected', {
                    topic: defaultTopic,
                    reason: 'missing_raw_payload'
                });
                res.status(400).json({ error: 'Expected a raw Shopify webhook payload.' });
                return;
            }
            if (!verifyWebhookSignature(rawBody, signature)) {
                (0, index_js_2.logWarning)('shopify_webhook_rejected', {
                    topic: defaultTopic,
                    reason: 'invalid_signature'
                });
                res.status(401).json({ error: 'Invalid Shopify webhook signature.' });
                return;
            }
            const shopDomain = normalizeShopDomain(req.header('x-shopify-shop-domain') ?? 'unknown.myshopify.com');
            const activeShopDomain = await getActiveInstalledShopDomain();
            if (activeShopDomain && activeShopDomain !== shopDomain) {
                (0, index_js_2.logWarning)('shopify_webhook_rejected', {
                    topic: defaultTopic,
                    reason: 'shop_domain_mismatch',
                    shopDomain
                });
                res.status(403).json({ error: 'Webhook shop domain does not match the connected store.' });
                return;
            }
            const rawPayload = JSON.parse(rawBody.toString('utf8'));
            const payload = shopifyOrderPayloadSchema.parse(rawPayload);
            await persistWebhook({
                payload,
                rawPayload,
                rawBody,
                topic: req.header('x-shopify-topic') ?? defaultTopic,
                shopDomain,
                webhookId: req.header('x-shopify-webhook-id') ?? null
            });
            res.status(200).json({ ok: true });
        }
        catch (error) {
            (0, index_js_2.logError)('shopify_webhook_failed', error, {
                topic: req.header('x-shopify-topic') ?? defaultTopic,
                webhookId: req.header('x-shopify-webhook-id') ?? null
            });
            next(error);
        }
    };
}
function createAppUninstalledWebhookHandler() {
    return async (req, res, next) => {
        try {
            const rawBody = req.body;
            const signature = req.header('x-shopify-hmac-sha256') ?? undefined;
            if (!Buffer.isBuffer(rawBody)) {
                (0, index_js_2.logWarning)('shopify_webhook_rejected', {
                    topic: 'app/uninstalled',
                    reason: 'missing_raw_payload'
                });
                res.status(400).json({ error: 'Expected a raw Shopify webhook payload.' });
                return;
            }
            if (!verifyWebhookSignature(rawBody, signature)) {
                (0, index_js_2.logWarning)('shopify_webhook_rejected', {
                    topic: 'app/uninstalled',
                    reason: 'invalid_signature'
                });
                res.status(401).json({ error: 'Invalid Shopify webhook signature.' });
                return;
            }
            const shopDomain = normalizeShopDomain(req.header('x-shopify-shop-domain') ?? 'unknown.myshopify.com');
            await markInstallationUninstalled(shopDomain);
            (0, index_js_2.logInfo)('shopify_app_uninstalled', {
                shopDomain
            });
            res.status(200).json({ ok: true });
        }
        catch (error) {
            (0, index_js_2.logError)('shopify_webhook_failed', error, {
                topic: 'app/uninstalled'
            });
            next(error);
        }
    };
}
function createShopifyPublicRouter() {
    const router = (0, express_1.Router)();
    router.get('/install', async (req, res, next) => {
        try {
            assertShopifyAppConfig();
            const input = installRequestSchema.parse(req.query);
            const shopDomain = normalizeShopDomain(input.shop);
            const returnTo = normalizeReturnTo(input.returnTo);
            await assertSingleStoreInstallAllowed(shopDomain);
            const state = (0, node_crypto_1.randomBytes)(32).toString('hex');
            await persistOAuthState(shopDomain, state, returnTo);
            res.redirect(302, buildShopifyInstallUrl(shopDomain, state, returnTo));
        }
        catch (error) {
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
            const webhookSubscriptions = await provisionWebhookSubscriptions(canonicalShopDomain, accessToken, webhookBaseUrl);
            await upsertShopifyInstallation({
                shopDomain: canonicalShopDomain,
                accessToken,
                scopes,
                webhookBaseUrl,
                webhookSubscriptions,
                shopIdentity
            });
            const redirectTarget = state.return_to ?? normalizeReturnTo(env_js_1.env.SHOPIFY_APP_POST_INSTALL_REDIRECT_URL) ?? null;
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
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
function createShopifyWebhookRouter() {
    const router = (0, express_1.Router)();
    router.post('/orders-create', createOrderWebhookHandler('orders/create'));
    router.post('/orders-paid', createOrderWebhookHandler('orders/paid'));
    router.post('/app-uninstalled', createAppUninstalledWebhookHandler());
    return router;
}
function createShopifyAdminRouter() {
    const router = (0, express_1.Router)();
    router.use(index_js_4.attachAuthContext);
    router.use(index_js_4.requireAdmin);
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
            let reconnectUrl = null;
            if (env_js_1.env.SHOPIFY_APP_API_KEY && env_js_1.env.SHOPIFY_APP_API_SECRET && env_js_1.env.SHOPIFY_APP_API_VERSION && env_js_1.env.SHOPIFY_APP_BASE_URL) {
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
        }
        catch (error) {
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
            const webhookSubscriptions = await provisionWebhookSubscriptions(installation.shop_domain, accessToken, webhookBaseUrl);
            await (0, pool_js_1.query)(`
          UPDATE shopify_app_installations
          SET
            webhook_base_url = $2,
            webhook_subscriptions = $3::jsonb,
            updated_at = now()
          WHERE shop_domain = $1
        `, [installation.shop_domain, webhookBaseUrl, JSON.stringify(webhookSubscriptions)]);
            res.status(200).json({
                ok: true,
                shopDomain: installation.shop_domain,
                webhookSubscriptions
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.post('/orders/backfill', async (req, res, next) => {
        try {
            assertShopifyAppConfig();
            const input = shopifyBackfillRequestSchema.parse(req.body ?? {});
            const installation = await getShopifyInstallationSummary();
            if (!installation || installation.status !== 'active') {
                throw new ShopifyHttpError(404, 'shopify_installation_not_found', 'No active Shopify installation was found');
            }
            const accessToken = await getActiveShopifyAccessToken(installation.shop_domain);
            const reportingTimezone = await (0, index_js_7.getReportingTimezone)();
            const result = await backfillShopifyOrders(installation.shop_domain, accessToken, reportingTimezone, input.startDate, input.endDate);
            res.status(200).json({
                ok: true,
                shopDomain: installation.shop_domain,
                startDate: input.startDate,
                endDate: input.endDate,
                ...result
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.post('/orders/recover-attribution', async (req, res, next) => {
        try {
            const input = shopifyBackfillRequestSchema.parse(req.body ?? {});
            const reportingTimezone = await (0, index_js_7.getReportingTimezone)();
            const result = await recoverShopifyAttributionHints(reportingTimezone, input.startDate, input.endDate);
            res.status(200).json({
                ok: true,
                startDate: input.startDate,
                endDate: input.endDate,
                ...result
            });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
exports.__shopifyTestUtils = {
    normalizeShopDomain,
    createOAuthHmacMessage,
    verifyShopifyOAuthHmac,
    buildShopifyInstallUrl,
    verifyWebhookSignature,
    isEligibleOnlineStoreOrder,
    buildLineItemExternalId,
    extractShopifyHintAttribution,
    recoverShopifyAttributionHints
};
