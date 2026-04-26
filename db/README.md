# Database Migrations

This directory contains PostgreSQL schema migrations for the ROAS Radar MVP backend.

## Current Schema

The `migrations/` directory contains the SQL migration history for the product schema. The migration runner records applied files in `schema_migrations` and executes pending `.sql` files in lexical order.

The `bootstrap/001_roles.sql` script applies the least-privilege grants expected by production:

- `roas_migrator` owns the `public` schema and performs DDL.
- `roas_app` can read and write application tables and sequences, but cannot create or alter schema objects.
- `roas_readonly` has read-only access for support and debugging use cases.

For ad-platform ingestion, treat `meta_ads_raw_spend_records` and `google_ads_raw_spend_records` as the canonical raw-source tables. The corresponding `*_daily_spend` tables are derived projections for reporting and reconciliation, not the source-of-truth raw payload store.

Run the bootstrap script once after provisioning the database and before deploying application services.

## Applying Migrations Locally

Run the migration runner from the repository root after setting `DATABASE_URL`:

## Rollback Files

Forward migrations in `db/migrations/` are the only files executed by `src/db/migrate.ts`.
Manual rollback SQL is stored separately in `db/rollbacks/` for operator use during incident response.

The session-attribution capture schema added in `0019_add_session_attribution_capture_tables.sql` can be rolled back with:

- `db/rollbacks/0019_add_session_attribution_capture_tables.down.sql`

## Session Attribution Capture Schema

Migration `0019_add_session_attribution_capture_tables.sql` adds three additive tables for canonical first-party capture persistence:

- `session_attribution_identities`: one row per `roas_radar_session_id`
- `session_attribution_touch_events`: event/touch history keyed by `roas_radar_session_id`
- `order_attribution_links`: normalized order-to-session linkage rows

Retention support is built into each table through a `retained_until` column initialized to `now() + interval '30 days'` plus pruning indexes on that column.

Primary lookup indexes added by the migration:

- session lookup: `session_attribution_touch_events_session_occurred_at_idx`
- event timestamp lookup: `session_attribution_touch_events_occurred_at_idx`
- order lookup: `order_attribution_links_order_lookup_idx`

To verify the PostgreSQL planner is using those indexes against a real database, run:

```sh
npm run db:verify:session-attribution-plans
```
