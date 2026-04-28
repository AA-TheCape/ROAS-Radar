# Cloud Run Deployment

This directory contains the checked-in deployment contract for the Node backend, the optional dashboard service, and the scheduled jobs that support attribution and ad-ingestion workloads.

## Managed workloads

- `roas-radar-api`: public backend service running `npm run start:api`
- `roas-radar-attribution-worker`: internal worker service running `npm run start:worker-service`
- `roas-radar-migrate`: one-shot migration job running `npm run db:migrate`
- `roas-radar-meta-ads-sync`: scheduled job running `npm run meta-ads:sync`
- `roas-radar-google-ads-sync`: scheduled job running `npm run google-ads:sync`
- `roas-radar-ga4-session-attribution`: scheduled GA4 ingestion job running `npm run ga4:ingest:start`
- `roas-radar-ga4-session-attribution-scheduler`: Cloud Scheduler HTTP job that executes the GA4 ingestion Cloud Run Job

## Files

- `environments/ENVIRONMENT.env`: template for a new environment
- `environments/staging.env`: staging defaults
- `environments/production.env`: production defaults
- `bootstrap-iam.sh`: creates service accounts and grants baseline project or secret access
- `deploy.sh`: builds images unless `SKIP_BUILDS=true`, deploys services and jobs, and upserts the GA4 scheduler

## Required environment values

Each environment file must define:

- `GCP_PROJECT_ID`
- `GCP_REGION`
- `ARTIFACT_REGISTRY_REPOSITORY`
- service and job names
- service-account names
- secret bindings for `DATABASE_URL` and any other runtime secrets
- `GA4_INGESTION_SCHEDULE`

The checked-in env files are valid shell files. Replace the placeholder project id, Cloud SQL connection name, VPC connector, and any extra secret bindings before deploying.

## Typical flow

```bash
infra/cloud-run/bootstrap-iam.sh staging
infra/cloud-run/deploy.sh staging
```

Common deploy flags:

- `SKIP_BUILDS=true`: reuse an already-pushed image tag instead of building locally
- `SHORT_SHA=<tag>` or `IMAGE_TAG=<tag>`: force the image tag that `deploy.sh` references
- `RUN_MIGRATIONS_ON_DEPLOY=true`: execute the migration job after deployment
- `APPLY_MONITORING_ON_DEPLOY=true`: apply the monitoring assets in `infra/monitoring/`

## GA4 hourly ingestion

The deploy contract for GA4 is:

1. Deploy the backend image that contains `dist/src/ga4-session-attribution-worker.js`.
2. Deploy the Cloud Run Job with `npm run ga4:ingest:start`.
3. Bind `GA4_BIGQUERY_ENABLED=true` plus the GA4 ingestion retry and stale-lock env vars.
4. Upsert the Cloud Scheduler job on `5 * * * *` unless the environment file says otherwise.

The worker itself handles hour-level retries and dead-lettering, so the job-level Cloud Run retry count stays at `0`.

## Verification after deploy

- `gcloud run services describe "$API_SERVICE_NAME" --region "$GCP_REGION"`
- `gcloud run services describe "$ATTRIBUTION_WORKER_SERVICE_NAME" --region "$GCP_REGION"`
- `gcloud run jobs describe "$GA4_INGESTION_JOB_NAME" --region "$GCP_REGION"`
- `gcloud scheduler jobs describe "$GA4_INGESTION_SCHEDULER_JOB_NAME" --location "$GCP_REGION"`
- `gcloud run jobs execute "$GA4_INGESTION_JOB_NAME" --region "$GCP_REGION" --wait`

For repeated GA4 failures, use `docs/runbooks/ga4-hourly-ingestion.md`. For staged fallback cutover, use `docs/runbooks/ga4-fallback-rollout.md`.
