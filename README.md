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

The Shopify theme snippet and asset under `shopify/theme/` are still the expected frontend mechanism for propagating `roas_radar_session_id` into cart and checkout attributes.
