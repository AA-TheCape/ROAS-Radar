# Cloud Run Pipelines Runbook

Use this runbook when deploying or operating the scheduled Cloud Run workers in staging or production.

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

## Verification

Run these checks after staging and production deploys.

1. Verify services:
   - `gcloud run services list --region "$GCP_REGION" --project "$GCP_PROJECT_ID"`
   - `gcloud run services describe "$API_SERVICE_NAME" --region "$GCP_REGION" --project "$GCP_PROJECT_ID"`
   - `gcloud run services describe "$WORKER_SERVICE_NAME" --region "$GCP_REGION" --project "$GCP_PROJECT_ID"`
2. Verify jobs:
   - `gcloud run jobs list --region "$GCP_REGION" --project "$GCP_PROJECT_ID"`
   - `gcloud run jobs executions list --region "$GCP_REGION" --project "$GCP_PROJECT_ID" --job "$IDENTITY_GRAPH_BACKFILL_JOB_NAME"`
   - `gcloud run jobs executions list --region "$GCP_REGION" --project "$GCP_PROJECT_ID" --job "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_NAME"`
3. Verify schedulers:
   - `gcloud scheduler jobs list --location "$GCP_REGION" --project "$GCP_PROJECT_ID"`
4. Execute the two identity-specific jobs once after deploy:
   - `gcloud run jobs execute "$IDENTITY_GRAPH_BACKFILL_JOB_NAME" --region "$GCP_REGION" --project "$GCP_PROJECT_ID" --wait`
   - `gcloud run jobs execute "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_NAME" --region "$GCP_REGION" --project "$GCP_PROJECT_ID" --wait`
5. Validate logs:
   - `identity_graph_backfill_worker_started`
   - `order_attribution_materialization_worker_started`
   - no repeated `*_failed` log entries for the same execution
6. Validate data health:
   - `GET /api/reporting/reconciliation?runDate=YYYY-MM-DD`
   - dashboard identity health view
   - ads sync and attribution alerts remain green in the monitoring dashboard

## Schedule Defaults

- Meta Ads sync: hourly at `15 * * * *`
- Google Ads sync: hourly at `45 * * * *`
- session retention: daily at `0 3 * * *`
- data quality: daily at `20 3 * * *`
- identity graph backfill: daily at `35 3 * * *`
- order attribution materialization: daily at `50 3 * * *`

All schedules use the environment's `ADS_SYNC_TIME_ZONE`.

## Least-Privilege IAM Expectations

- API and attribution worker service accounts can access Cloud SQL and only the runtime secrets they need.
- migration uses the migrator DSN and does not share that credential with services or recurring jobs.
- Meta Ads and Google Ads jobs each have their own service account and only their own encryption or app-secret access.
- data quality, retention, and identity graph backfill only need `DATABASE_URL`.
- order attribution materialization needs `DATABASE_URL` plus `SHOPIFY_APP_ENCRYPTION_KEY` for writeback.
- Cloud Scheduler uses a dedicated invoker service account with `roles/run.invoker`.

## Common Failures

- Scheduler invokes fail with `403`: check that the scheduler invoker service account still has `roles/run.invoker`.
- Job starts but cannot read secrets: inspect Secret Manager IAM for that workload's dedicated service account.
- Job starts but cannot connect to Postgres: verify `roles/cloudsql.client`, Cloud SQL instance attachment, and `DATABASE_URL`.
- identity graph backfill runs too long: reduce `IDENTITY_GRAPH_BACKFILL_LOOKBACK_DAYS` or `IDENTITY_GRAPH_BACKFILL_MAX_BATCHES`.
- order attribution materialization writes back unexpectedly: set `ORDER_ATTRIBUTION_MATERIALIZATION_SKIP_SHOPIFY_WRITEBACK=true` and redeploy.

## Rollback

1. Pause the affected scheduler:
   - `gcloud scheduler jobs pause "$JOB_NAME" --location "$GCP_REGION" --project "$GCP_PROJECT_ID"`
2. Roll back the Cloud Run service or job to the prior image tag:
   - `gcloud run services update "$SERVICE_NAME" --image "$PREVIOUS_IMAGE" --region "$GCP_REGION" --project "$GCP_PROJECT_ID"`
   - `gcloud run jobs deploy "$JOB_NAME" --image "$PREVIOUS_IMAGE" --region "$GCP_REGION" --project "$GCP_PROJECT_ID"`
3. Re-run the specific job manually if the failed execution left work partially processed.
4. Resume the scheduler after validation:
   - `gcloud scheduler jobs resume "$JOB_NAME" --location "$GCP_REGION" --project "$GCP_PROJECT_ID"`
