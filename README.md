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
- `SHOPIFY_WEBHOOK_SECRET`
- `REPORTING_API_TOKEN`

Optional environment variables:

- `PORT`
- `ATTRIBUTION_WINDOW_DAYS`

## Existing Shopify storefront assets

The Shopify theme snippet and assets under `shopify/theme/` are the expected frontend mechanism for:

- creating and persisting the first-party `_hba_id` tracking cookie for 365 days,
- sending a `page_view` payload to `/track` on page load with `sendBeacon` plus fetch/XMLHttpRequest fallback and retry queueing,
- propagating the same UUID into `roas_radar_session_id` cart and checkout attributes for order attribution.
