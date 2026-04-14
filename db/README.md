# Database Migrations

This directory contains PostgreSQL schema migrations for the ROAS Radar MVP backend.

## Current Schema

The `migrations/` directory contains the SQL migration history for the product schema. The migration runner records applied files in `schema_migrations` and executes pending `.sql` files in lexical order.

The `bootstrap/001_roles.sql` script applies the least-privilege grants expected by production:

- `roas_migrator` owns the `public` schema and performs DDL.
- `roas_app` can read and write application tables and sequences, but cannot create or alter schema objects.
- `roas_readonly` has read-only access for support and debugging use cases.

Run the bootstrap script once after provisioning the database and before deploying application services.

## Applying Migrations Locally

Run the migration runner from the repository root after setting `DATABASE_URL`:

