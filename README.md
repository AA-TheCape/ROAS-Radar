# ROAS Radar

ROAS Radar is a Shopify attribution and reporting product built around a Node.js API, PostgreSQL, and a React dashboard.

## Backend scaffold

This repository now includes an MVP backend foundation with:

- an Express API service for `/track`, Shopify order webhooks, and authenticated reporting endpoints,
- a PostgreSQL migration runner,
- an attribution worker entrypoint for batch order attribution,
- the MVP analytics schema under `db/migrations`.

## Commands

```bash
npm install
npm run db:migrate
npm run dev
```

Required environment variables:

- `DATABASE_URL`
- `REPORTING_API_TOKEN`

Optional environment variables:

- `PORT`
- `ATTRIBUTION_WINDOW_DAYS`
- `SHOPIFY_WEBHOOK_SECRET`
- `SHOPIFY_APP_API_KEY`
- `SHOPIFY_APP_API_SECRET`
- `SHOPIFY_APP_API_VERSION`
- `SHOPIFY_APP_BASE_URL`
- `SHOPIFY_APP_ENCRYPTION_KEY`
- `SHOPIFY_APP_SCOPES`
- `SHOPIFY_APP_POST_INSTALL_REDIRECT_URL`

## Existing Shopify storefront assets

The Shopify theme snippet and assets under `shopify/theme/` are the expected frontend mechanism for:

- creating and persisting the first-party `_hba_id` tracking cookie for 365 days,
- sending a `page_view` payload to `/track` on page load with `sendBeacon` plus fetch/XMLHttpRequest fallback and retry queueing,
- propagating the same UUID into `roas_radar_session_id` cart and checkout attributes for order attribution.

## Identity Stitching

Visitor identity stitching is documented in [docs/visitor-identity-stitching.md](docs/visitor-identity-stitching.md).

The backend now creates canonical customer identities from hashed email plus Shopify customer id, links historical tracked sessions when checkout evidence is present, and refuses automatic merges when those identifiers disagree with already-linked records.

## Shopify OAuth Setup

Shopify OAuth installation, encrypted Admin API credential storage, and automatic webhook provisioning are documented in [docs/shopify-app-setup.md](docs/shopify-app-setup.md).
