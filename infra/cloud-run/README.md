# Cloud Run Deployment

This directory contains the operational scripts and environment definitions for deploying the ROAS Radar API, dashboard, attribution worker, and migration job to Google Cloud Run.

## Topology

The deployment flow assumes four deployable workloads:

- `roas-radar-api`: public Cloud Run service for `/track`, Shopify webhooks, and authenticated reporting APIs.
- `roas-radar-dashboard`: public Cloud Run service for the React reporting dashboard.
- `roas-radar-attribution-worker`: internal Cloud Run service for attribution and asynchronous processing.
- `roas-radar-migrate`: Cloud Run Job that runs `npm run db:migrate:start` with elevated database credentials.

The API and worker use the `roas_app` PostgreSQL login. The migration job uses the `roas_migrator` PostgreSQL login. Do not reuse the migrator credential in long-lived application services.

## Required Secrets

The deploy script expects the following Secret Manager secrets to exist for each environment:

- `DATABASE_URL`: runtime DSN for the API and worker (`roas_app` user).
- `MIGRATOR_DATABASE_URL`: DSN for the migration job (`roas_migrator` user).
- `REPORTING_API_TOKEN`
- `SHOPIFY_WEBHOOK_SECRET`
- `SHOPIFY_APP_API_KEY`
- `SHOPIFY_APP_API_SECRET`
- `SHOPIFY_APP_ENCRYPTION_KEY`
- `META_ADS_APP_SECRET`
- `META_ADS_ENCRYPTION_KEY`
- `GOOGLE_ADS_ENCRYPTION_KEY`

The environment files also carry non-secret runtime settings that must be populated before deployment, including:

- `TRACKING_ALLOWED_ORIGINS`
- `SHOPIFY_APP_BASE_URL`
- `SHOPIFY_APP_API_VERSION`
- `SHOPIFY_APP_SCOPES`
- `SHOPIFY_APP_POST_INSTALL_REDIRECT_URL`
- `DASHBOARD_API_BASE_URL`

## First-Time Setup

1. Provision Cloud SQL and private networking from `infra/cloud-sql/`.
2. Run `infra/cloud-run/bootstrap-iam.sh ENVIRONMENT` to create service accounts and grant IAM roles.
3. Create the environment secrets in Secret Manager.
4. Populate `infra/cloud-run/environments/ENVIRONMENT.env`.
5. Deploy with `infra/cloud-run/deploy.sh ENVIRONMENT`.
6. Apply monitoring with `infra/monitoring/apply.sh ENVIRONMENT`.

## Deployment
