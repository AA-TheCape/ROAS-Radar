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

The confidence metadata expand migration added in `0046_add_order_attribution_confidence_metadata.sql` can be rolled back with:

- `db/rollbacks/0046_add_order_attribution_confidence_metadata.down.sql`

Do not use that rollback after the contract operation has been applied unless application traffic is first pinned to a revision that does not reference the confidence metadata columns.

The attribution-engine v1 tables added in `0040_add_attribution_engine_v1_tables.sql` can be rolled back with:

- `db/rollbacks/0040_add_attribution_engine_v1_tables.down.sql`

The Meta order-value aggregate table added in `0040_add_meta_order_value_aggregates.sql` can be rolled back with:

- `db/rollbacks/0040_add_meta_order_value_aggregates.down.sql`

The recovery job registry added in `0045_add_recovery_run_registry.sql` can be rolled back with:

- `db/rollbacks/0045_add_recovery_run_registry.down.sql`

The shared recovery job queue fields added in `0048_add_shared_recovery_job_queue.sql` can be rolled back with:

- `db/rollbacks/0048_add_shared_recovery_job_queue.down.sql`

The Meta metadata cache table added in `0053_add_meta_metadata_cache.sql` can be rolled back with:

- `db/rollbacks/0053_add_meta_metadata_cache.down.sql`

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

## Recovery Job Registry

Migration `0045_add_recovery_run_registry.sql` adds generic run tracking for automatic backfill and recovery workflows:

- `recovery_job_runs`: run registry with job type, time range, initiator, mode, dry-run flag, counters, checkpoints, heartbeat fields, and resume/rerun links
- `recovery_job_records`: per-record processing status with attempt tracking and a unique `side_effect_key` for idempotent resume/rerun behavior
- `recovery_job_checkpoints`: named cursors and high-water marks for resumable scans
- `recovery_job_errors`: run and record-scoped error audit log
- `recovery_job_status_events`: status transition audit trail

The registry includes descending timestamp indexes and job/status/initiator composites intended for last-30-days operational lookups.

Migration `0048_add_shared_recovery_job_queue.sql` extends the registry with worker queue semantics:

- queue claim ordering through `priority` and `available_at`
- run-level attempt limits and retry backoff state
- heartbeat expiration through `lock_expires_at`
- `dead_lettered` terminal state and replay through `event_dead_letters`
- durable completion reports in `recovery_job_completion_reports`

## Confidence Metadata Rollout

Migration `0046_add_order_attribution_confidence_metadata.sql` is intentionally expand-only for Cloud SQL. It adds nullable metadata columns and `NOT VALID` constraints, but it does not backfill historical orders, validate constraints, enforce `NOT NULL`, or create large indexes.

Production rollout order:

1. Expand: run `npm run db:migrate` or the `roas-radar-migrate` Cloud Run Job.
2. Deploy: roll API, worker, dashboard, and jobs that can read nullable historical metadata and write complete metadata for new rows.
3. Backfill: run `npm run attribution:backfill-confidence -- --dry-run --batch-size 1000`, then the write-enabled command. Resume with `--resume-after-order-row-id <cursor>` if interrupted.
4. Index: run `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/operations/0046_add_order_attribution_confidence_indexes.sql`. This uses `CREATE INDEX CONCURRENTLY` because the standard migration runner wraps SQL in a transaction.
5. Verify: confirm the backfill report completed and the contract preflight query in `db/operations/0046_contract_order_attribution_confidence_metadata.sql` would return zero incomplete orders, results, and credits.
6. Contract: run `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/operations/0046_contract_order_attribution_confidence_metadata.sql` to validate constraints and enforce `NOT NULL`.

Rollback behavior:

- Before contract: route Cloud Run services back to the previous revision if needed. Leave the expanded schema in place; it is backward-compatible. Use the rollback SQL only after confirming no running revision uses the new columns.
- During backfill: stop the job and resume later from the last reported cursor. The backfill is idempotent and bounded by order row ID.
- During concurrent index creation: canceling may leave an invalid index. Drop the specific invalid index with `DROP INDEX CONCURRENTLY IF EXISTS <index_name>` and rerun the index operation.
- After contract: rollback requires a forward fix or a planned maintenance rollback because dropping `NOT NULL`/validated constraints and columns can conflict with deployed code that now depends on complete metadata.
