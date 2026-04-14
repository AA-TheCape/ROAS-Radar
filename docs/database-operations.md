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
