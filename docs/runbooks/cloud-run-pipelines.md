# Cloud Run Pipelines Runbook

Use this runbook when deploying or operating the scheduled Cloud Run workers in dev, staging, or production.

## Managed Workloads

- Cloud Run services:
  - `roas-radar-api`
  - `roas-radar-dashboard`
  - `roas-radar-attribution-worker`
- Cloud Run Jobs:
  - `roas-radar-migrate`
  - `roas-radar-meta-ads-sync`
  - `roas-radar-google-ads-sync`
  - `roas-radar-session-retention`
  - `roas-radar-data-quality`
  - `roas-radar-identity-graph-backfill`
  - `roas-radar-order-attribution-materialization`
- Cloud Scheduler:
  - one scheduler per recurring Cloud Run Job

## First-Time Bootstrap

1. Fill in `infra/cloud-run/environments/<environment>.env`.
2. Run `sh infra/cloud-run/bootstrap-iam.sh <environment>`.
3. Create or rotate the required Secret Manager secrets:
   - `DATABASE_URL`
   - `MIGRATOR_DATABASE_URL`
   - `REPORTING_API_TOKEN`
   - `SHOPIFY_WEBHOOK_SECRET`
   - `SHOPIFY_APP_API_KEY`
   - `SHOPIFY_APP_API_SECRET`
   - `SHOPIFY_APP_ENCRYPTION_KEY`
   - `META_ADS_APP_SECRET`
   - `META_ADS_ENCRYPTION_KEY`
   - `GOOGLE_ADS_ENCRYPTION_KEY`
4. Deploy with `sh infra/cloud-run/deploy.sh <environment>`.

For staged releases, prefer:

