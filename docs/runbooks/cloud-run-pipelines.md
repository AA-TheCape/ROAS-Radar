# Cloud Run Pipelines

Use this runbook for deploy, rollback, and post-deploy verification of the Cloud Run services and jobs that make up ROAS Radar.

## Managed workloads

- `roas-radar-api`
- `roas-radar-attribution-worker`
- `roas-radar-migrate`
- `roas-radar-meta-ads-sync`
- `roas-radar-google-ads-sync`
- `roas-radar-ga4-session-attribution`
- `roas-radar-ga4-session-attribution-scheduler`

## Pre-deploy checks

Run the backend verification commands from a clean Node 22 install:

```bash
npm ci --include=dev
npm run build
npm run test:unit
npm run test:integration
npm run test:attribution
```

If the deploy is driven through GitHub Actions, confirm the same branch already passed `.github/workflows/ci.yml`.

## Deploy

1. Confirm the target environment file under `infra/cloud-run/environments/` has the correct project id, runtime names, secret bindings, and GA4 schedule.
2. Bootstrap or reconcile IAM:

```bash
infra/cloud-run/bootstrap-iam.sh staging
```

3. Deploy the workloads:

```bash
RUN_MIGRATIONS_ON_DEPLOY=true APPLY_MONITORING_ON_DEPLOY=true infra/cloud-run/deploy.sh staging
```

## Post-deploy verification

Verify the current revisions and scheduler wiring:

```bash
gcloud run services describe roas-radar-api-staging --region us-central1
gcloud run services describe roas-radar-attribution-worker-staging --region us-central1
gcloud run jobs describe roas-radar-ga4-session-attribution-staging --region us-central1
gcloud scheduler jobs describe roas-radar-ga4-session-attribution-scheduler-staging --location us-central1
```

Execute the two one-shot runtime checks that matter most for this branch:

```bash
gcloud run jobs execute roas-radar-migrate-staging --region us-central1 --wait
gcloud run jobs execute roas-radar-ga4-session-attribution-staging --region us-central1 --wait
```

Then review logs for:

- `ga4_session_attribution_worker_started`
- `ga4_session_attribution_worker_failed`
- `order_attribution_backfill_job_lifecycle`
- `shopify_order_writeback_*`

## IAM expectations

At minimum:

- API and worker identities need Cloud SQL client plus logging and metric writer roles.
- Migration, Meta Ads sync, Google Ads sync, and GA4 ingestion jobs need Cloud SQL client.
- GA4 ingestion also needs BigQuery job execution and read access.
- The GA4 scheduler identity must be allowed to execute the Cloud Run Job through the Run API.
- Any runtime identity that reads `DATABASE_URL` or `REPORTING_API_TOKEN` must have Secret Manager accessor on those secrets.

## Rollback

1. Re-point traffic or redeploy the previous image tag with `SKIP_BUILDS=true SHORT_SHA=<old-tag> infra/cloud-run/deploy.sh <environment>`.
2. If the problem is isolated to GA4 ingestion, pause the scheduler first:

```bash
gcloud scheduler jobs pause roas-radar-ga4-session-attribution-scheduler-staging --location us-central1
```

3. Re-run the older GA4 job image only after verifying the previous config still matches the currently expected env contract.

For persistent GA4 job failures, continue in `docs/runbooks/ga4-hourly-ingestion.md`.
