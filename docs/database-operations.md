# Database Operations

This document defines the MVP operating model for Cloud SQL, migrations, and restore handling.

## Provisioning Standard

- Platform: Cloud SQL for PostgreSQL 16.
- Connectivity: private IP only through private service networking.
- Runtime path: Cloud Run services connect through the Cloud SQL connector.
- Environments: separate Cloud SQL instance per environment, with separate credentials and secrets.

## Least-Privilege Access Model

- `roas_migrator`: used only by the Cloud Run migration job and local/admin migration sessions. Owns the `public` schema and performs DDL.
- `roas_app`: used by the API and attribution worker. Limited to DML and sequence usage.
- `roas_readonly`: optional support/debugging login with `SELECT` only.

Application services must never use `roas_migrator`. Deployment automation must run migrations with `MIGRATOR_DATABASE_URL`, then deploy runtime services with `DATABASE_URL`.

## Backup And Restore Policy

- Automated backups: enabled daily at `03:00 UTC`.
- Backup retention: 14 retained backups.
- Point-in-time recovery: enabled with 7 days of transaction logs.
- Deletion protection: enabled on production instances.
- Maintenance window: Sundays at `04:00 UTC`.

## Restore Procedure

1. Identify the target restore timestamp or backup.
2. Create a new Cloud SQL instance from backup or point-in-time recovery.
3. Recreate the `DATABASE_URL` and `MIGRATOR_DATABASE_URL` secrets for the restored instance.
4. Run smoke checks:
   - `SELECT COUNT(*) FROM schema_migrations;`
   - `SELECT now();`
   - `/health` and a reporting API query against a temporary service revision.
5. Cut application traffic to the restored instance only after validation.

Do not restore in place for production unless the outage model requires it. Prefer restoring to a new instance, validating, and then switching secrets and service revisions.

## Migration Pipeline

- Pull requests: GitHub Actions runs build, tests, and `npm run db:migrate` against ephemeral PostgreSQL.
- Deployments: GitHub Actions calls `infra/cloud-run/deploy.sh`, which deploys and executes the Cloud Run migration job.
- Safety controls:
  - migration runner uses a PostgreSQL advisory transaction lock,
  - migration job is single-task and single-parallelism,
  - runtime services do not have DDL credentials.

## Session Attribution Capture Operations

Migration `0019_add_session_attribution_capture_tables.sql` adds canonical session-attribution capture tables for:

- first-party session identity keyed by `roas_radar_session_id`
- touch event persistence keyed by session and event time
- order-to-session attribution linkage keyed by `shopify_order_id`

The migration bakes in 30-day retention support with `retained_until` columns and matching pruning indexes.
Operational pruning can delete expired rows in this order:

1. `DELETE FROM order_attribution_links WHERE retained_until < now();`
2. `DELETE FROM session_attribution_touch_events WHERE retained_until < now();`
3. `DELETE FROM session_attribution_identities WHERE retained_until < now();`

Verify primary lookup plans after migration on each environment:

```sh
npm run db:verify:session-attribution-plans
```

The verification script seeds representative data inside a transaction, runs `EXPLAIN (FORMAT JSON)` for the primary session, order, and event-time queries, and fails if PostgreSQL does not choose:

- `session_attribution_touch_events_session_occurred_at_idx`
- `order_attribution_links_order_lookup_idx`
- `session_attribution_touch_events_occurred_at_idx`

If rollback is required, apply:

```sh
psql "$MIGRATOR_DATABASE_URL" -f db/rollbacks/0019_add_session_attribution_capture_tables.down.sql
```
