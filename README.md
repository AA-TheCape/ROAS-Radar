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
- `META_ADS_APP_ID`
- `META_ADS_APP_SECRET`
- `META_ADS_APP_BASE_URL`
- `META_ADS_APP_SCOPES`
- `META_ADS_API_VERSION`
- `META_ADS_ENCRYPTION_KEY`
- `META_ADS_AD_ACCOUNT_ID`
- `META_ADS_SYNC_LOOKBACK_DAYS`
- `META_ADS_SYNC_INITIAL_LOOKBACK_DAYS`
- `META_ADS_SYNC_BATCH_SIZE`
- `META_ADS_SYNC_MAX_RETRIES`
- `META_ADS_WORKER_LOOP`
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

## Meta Ads Spend Sync

The backend now includes a Meta Ads OAuth connection flow plus a retryable daily spend sync worker:

- `GET /api/meta-ads/oauth/start` creates a short-lived OAuth state and returns the Meta authorization URL.
- `GET /meta-ads/oauth/callback` exchanges the authorization code for a long-lived token, stores it encrypted in PostgreSQL, and records account metadata.
- `POST /api/meta-ads/sync` can enqueue a manual backfill date range.
- `npm run meta-ads:sync` processes the queue, refreshes tokens when nearing expiry, fetches daily spend at account/campaign/adset/ad level, enriches ad rows with creative metadata, and writes both raw and normalized spend tables.

In production, run `npm run meta-ads:sync` from a Cloud Run Job or scheduled worker once per day. The worker will automatically enqueue the configured rolling lookback window and retry API failures with exponential backoff.
