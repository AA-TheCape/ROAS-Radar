# Cloud Run Deployment

This directory contains the operational scripts and environment definitions for deploying the ROAS Radar API, dashboard, attribution worker, migration job, and scheduled ad spend sync jobs to Google Cloud Run.

## Topology

The deployment flow assumes seven deployable workloads plus three Cloud Scheduler triggers:

- `roas-radar-api`: public Cloud Run service for `/track`, Shopify webhooks, and authenticated reporting APIs.
- `roas-radar-dashboard`: public Cloud Run service for the React reporting dashboard.
- `roas-radar-attribution-worker`: internal Cloud Run service for attribution and asynchronous processing.
- `roas-radar-migrate`: Cloud Run Job that runs `npm run db:migrate:start` with elevated database credentials.
- `roas-radar-meta-ads-sync`: Cloud Run Job that runs `npm run meta-ads:sync:start` once per invocation.
- `roas-radar-google-ads-sync`: Cloud Run Job that runs `npm run google-ads:sync:start` once per invocation.
- `roas-radar-session-retention`: Cloud Run Job that runs `npm run session-attribution:retention:start` to prune expired attribution-session records.
- `roas-radar-meta-ads-sync-scheduler`: Cloud Scheduler job that invokes the Meta Ads Cloud Run Job.
- `roas-radar-google-ads-sync-scheduler`: Cloud Scheduler job that invokes the Google Ads Cloud Run Job.
- `roas-radar-session-retention-scheduler`: Cloud Scheduler job that invokes the session-retention Cloud Run Job.

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
- `META_ADS_JOB_NAME`
- `GOOGLE_ADS_JOB_NAME`
- `META_ADS_SCHEDULER_JOB_NAME`
- `GOOGLE_ADS_SCHEDULER_JOB_NAME`
- `RETENTION_JOB_NAME`
- `RETENTION_SCHEDULER_JOB_NAME`
- `RETENTION_JOB_SERVICE_ACCOUNT_NAME`
- `ADS_SYNC_DATABASE_POOL_MAX`
- `ADS_SYNC_TIME_ZONE`
- `META_ADS_SYNC_SCHEDULE`
- `GOOGLE_ADS_SYNC_SCHEDULE`
- `RETENTION_SCHEDULE`
- `SESSION_ATTRIBUTION_RETENTION_DAYS`
- `SESSION_ATTRIBUTION_RETENTION_BATCH_SIZE`
- `SESSION_ATTRIBUTION_RETENTION_MAX_BATCHES`

## First-Time Setup

1. Provision Cloud SQL and private networking from `infra/cloud-sql/`.
2. Run `infra/cloud-run/bootstrap-iam.sh ENVIRONMENT` to create service accounts and grant IAM roles.
3. Create the environment secrets in Secret Manager.
4. Populate `infra/cloud-run/environments/ENVIRONMENT.env`.
5. Deploy with `infra/cloud-run/deploy.sh ENVIRONMENT`.
6. Apply monitoring with `infra/monitoring/apply.sh ENVIRONMENT`.

## Deployment

For manual deployments, run `infra/cloud-run/deploy.sh staging` or `infra/cloud-run/deploy.sh production`.

If you want Cloud Build to deploy staging automatically from `main`, use `cloudbuild.staging.yaml` as the trigger build config. The build config:

- builds and pushes the API image,
- builds and pushes the dashboard image,
- reuses `infra/cloud-run/deploy.sh staging` with `SKIP_BUILDS=true`,
- optionally runs migrations and monitoring apply during the triggered deploy.

The Cloud Build trigger should run as the environment deployer service account created by `bootstrap-iam.sh`:

- `roas-radar-deployer-staging@<project>.iam.gserviceaccount.com` for staging
- `roas-radar-deployer-prod@<project>.iam.gserviceaccount.com` for production

Recommended trigger settings:

- event: push to branch
- branch regex: `^main$`
- region: the same region used by Cloud Run and Artifact Registry
- build config: `cloudbuild.staging.yaml`
