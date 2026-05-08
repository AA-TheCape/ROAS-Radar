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

The order-attribution tier audit columns added in `0037_add_shopify_order_attribution_tiers.sql` can be rolled back with:

- `db/rollbacks/0037_add_shopify_order_attribution_tiers.down.sql`

The attribution-engine v1 tables added in `0040_add_attribution_engine_v1_tables.sql` can be rolled back with:

- `db/rollbacks/0040_add_attribution_engine_v1_tables.down.sql`

The Meta order-value aggregate table added in `0040_add_meta_order_value_aggregates.sql` can be rolled back with:

- `db/rollbacks/0040_add_meta_order_value_aggregates.down.sql`

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

## Attribution Engine V1 Storage

Migration `0040_add_attribution_engine_v1_tables.sql` adds run-scoped storage for the new attribution engine:

- `attribution_runs`: run metadata and lookback contract
- `attribution_order_inputs`: normalized per-run order snapshots
- `attribution_touchpoint_inputs`: normalized per-run touchpoint candidates
- `attribution_model_summaries`: one summary row per order and model
- `attribution_model_credits`: non-zero credit rows per order, model, and touchpoint
- `attribution_explain_records`: explainability and audit trail rows

The scale policy for these tables is retention-driven rather than partition-driven in v1:

- run and result rows default to `400 days` retention
- normalized touchpoint and explainability rows default to `180 days` retention
- every retained table has a `retained_until` index so a pruning job can delete expired batches without scanning the full table

To verify the primary lookup indexes and reporting filters used by the new schema, run:

```sh
npm run db:verify-attribution-v1-query-plans
```
