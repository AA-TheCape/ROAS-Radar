# Database Migrations

This directory contains PostgreSQL schema migrations for the ROAS Radar MVP backend.

## Current schema

`migrations/0001_create_roas_radar_core_schema.sql` creates the launch schema defined in the MVP architecture artifact:

- `tracking_sessions`
- `tracking_events`
- `shopify_customers`
- `shopify_orders`
- `attribution_results`
- `shopify_webhook_receipts`
- `daily_campaign_metrics`

## Applying migrations

Run the migration runner from the repository root after setting `DATABASE_URL`:

```bash
npm run db:migrate
```

The runner records applied files in `schema_migrations` and executes `.sql` files in lexical order.
