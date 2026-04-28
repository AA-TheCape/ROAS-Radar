# Cloud Run Deployment

This directory contains the checked-in deployment contract for the Node backend, the optional dashboard service, and the scheduled jobs that support attribution and ad-ingestion workloads.

The root backend `Dockerfile` is the production packaging path for every backend Cloud Run workload in this directory. It builds on `node:22-bookworm-slim` and defaults the API container command to `npm run start:api`.

## Managed workloads

- `roas-radar-api`: public backend service running the image default command `npm run start:api`
- `roas-radar-attribution-worker`: internal worker service running `npm run start:worker-service`
- `roas-radar-migrate`: one-shot migration job running `npm run db:migrate`
- `roas-radar-meta-ads-sync`: scheduled job running `npm run meta-ads:sync`
- `roas-radar-google-ads-sync`: scheduled job running `npm run google-ads:sync`
- `roas-radar-ga4-session-attribution`: scheduled GA4 ingestion job running `npm run ga4:ingest:start`
- `roas-radar-ga4-session-attribution-scheduler`: Cloud Scheduler HTTP job that executes the GA4 ingestion Cloud Run Job through the Run Jobs API

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

## Required pre-deploy verification

Run these commands from the repo root on Node 22 before deploying:

```bash
npm ci --include=dev
npm run build
npm run start:api
npm run ga4:ingest:start
npm run db:migrate:check
npm run test:unit
npm run test:integration
npm run test:attribution
docker build -t roas-radar .
```

This sequence validates the compiled backend entrypoints, the migration check, the GA4-critical test suite, and the Docker packaging path that Cloud Run consumes.

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
2. Deploy the Cloud Run Job with command `npm` and args `run,ga4:ingest:start`.
3. Bind `GA4_BIGQUERY_ENABLED=true` plus `GA4_INGESTION_REQUESTED_BY`, batch size, retry, backoff, and stale-lock env vars.
4. Upsert the Cloud Scheduler job on `5 * * * *` unless the environment file says otherwise.
5. Keep the Cloud Run Job retry count at `0`; the application owns hour-level retry and dead-letter behavior.

The checked-in environment files set:

- `GA4_INGESTION_REQUESTED_BY=cloud-run-scheduler-<environment>`
- `GA4_INGESTION_BATCH_SIZE=6`
- `GA4_INGESTION_MAX_RETRIES=5`
- `GA4_INGESTION_INITIAL_BACKOFF_SECONDS=300`
- `GA4_INGESTION_MAX_BACKOFF_SECONDS=21600`
- `GA4_INGESTION_STALE_LOCK_MINUTES=30`
- `GA4_INGESTION_SCHEDULER_TIME_ZONE=Etc/UTC`

## Verification after deploy

- `gcloud run services describe "$API_SERVICE_NAME" --region "$GCP_REGION"`
- `gcloud run services describe "$ATTRIBUTION_WORKER_SERVICE_NAME" --region "$GCP_REGION"`
- `gcloud run jobs describe "$GA4_INGESTION_JOB_NAME" --region "$GCP_REGION"`
- `gcloud scheduler jobs describe "$GA4_INGESTION_SCHEDULER_JOB_NAME" --location "$GCP_REGION"`
- `gcloud run jobs execute "$MIGRATION_JOB_NAME" --region "$GCP_REGION" --wait`
- `gcloud run jobs execute "$GA4_INGESTION_JOB_NAME" --region "$GCP_REGION" --wait`

Check that the job description still shows command `npm`, args `run,ga4:ingest:start`, and `maxRetries: 0`.

For repeated GA4 failures, use `docs/runbooks/ga4-hourly-ingestion.md`. For staged fallback cutover, use `docs/runbooks/ga4-fallback-rollout.md`.
