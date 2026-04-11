# Shopify App Setup

ROAS Radar now supports a single-store Shopify app install flow for OAuth, secure credential storage, and webhook provisioning.

## Required environment

Set these variables before starting the API service:

- `DATABASE_URL`
- `REPORTING_API_TOKEN`
- `SHOPIFY_APP_API_KEY`
- `SHOPIFY_APP_API_SECRET`
- `SHOPIFY_APP_API_VERSION`
- `SHOPIFY_APP_BASE_URL`
- `SHOPIFY_APP_ENCRYPTION_KEY`

Optional:

- `SHOPIFY_APP_SCOPES`
  Default: `read_orders`
- `SHOPIFY_APP_POST_INSTALL_REDIRECT_URL`
  Absolute dashboard URL or root-relative path to redirect after OAuth completes
- `SHOPIFY_WEBHOOK_SECRET`
  If omitted, webhook verification falls back to `SHOPIFY_APP_API_SECRET`

## Shopify Partner Dashboard configuration

Create a custom app and configure:

- App URL: `${SHOPIFY_APP_BASE_URL}/shopify/install`
- Allowed redirection URL: `${SHOPIFY_APP_BASE_URL}/shopify/oauth/callback`
- Required Admin API scopes:
  At minimum `read_orders`, plus any additional scopes passed in `SHOPIFY_APP_SCOPES`

## First connection flow

1. Deploy the API with the environment values above.
2. Run database migrations so `shopify_app_installations` and `shopify_oauth_states` exist.
3. Start the install flow by opening:

```text
GET /shopify/install?shop=<store>.myshopify.com
```

4. After Shopify redirects back to `/shopify/oauth/callback`, ROAS Radar:
   - verifies the HMAC callback signature,
   - exchanges the authorization code for an Admin API token,
   - encrypts and stores the token in PostgreSQL using `pgp_sym_encrypt`,
   - fetches shop metadata,
   - auto-registers `orders/create`, `orders/paid`, and `app/uninstalled` webhooks.

5. Confirm the connection through:

```text
GET /api/shopify/connection
Authorization: Bearer <REPORTING_API_TOKEN>
```

## Reconnect flow

Use the same install endpoint with the already connected shop:

```text
GET /shopify/install?shop=<store>.myshopify.com
```

The existing row is updated in place, the encrypted access token is rotated, and webhook subscriptions are re-provisioned.

For operational recovery without reinstalling the app, you can also re-sync webhook subscriptions with:

```text
POST /api/shopify/webhooks/sync
Authorization: Bearer <REPORTING_API_TOKEN>
```

## Single-store behavior

- Only one active Shopify store can be connected in this MVP environment.
- Attempting to connect a different active store returns `409`.
- `app/uninstalled` marks the stored installation as `uninstalled`, which makes reconnecting the same store explicit and auditable.
