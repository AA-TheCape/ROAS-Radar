# Cloud Run Pipelines

Use this runbook when deploying or operating the scheduled Cloud Run workers in dev, staging, or production.

## Managed workloads

- Cloud Run services:
  - `roas-radar-api`
  - `roas-radar-dashboard`
  - `roas-radar-attribution-worker`
- Cloud Run Jobs:
  - `roas-radar-migrate`
  - `roas-radar-meta-ads-sync`
  - `roas-radar-meta-order-value-sync`
  - `roas-radar-meta-ads-metadata-refresh`
  - `roas-radar-google-ads-metadata-refresh`
  - `roas-radar-google-ads-sync`
  - `roas-radar-session-retention`
  - `roas-radar-data-quality`
  - `roas-radar-identity-graph-backfill`
  - `roas-radar-order-attribution-materialization`
  - `roas-radar-mmm-baseline`
  - `roas-radar-mmm-bayesian`
- Cloud Scheduler:
  - one scheduler per recurring Cloud Run Job

## Pre-deploy checks

Run the backend verification contract from a clean Node 22.12.0+ checkout in this order:

1. Confirm the target environment infrastructure plan has no unexpected drift:
   `terraform -chdir=infra/terraform/gcp-pipeline plan -var-file=environments/<environment>.tfvars`
2. Confirm Secret Manager has current versions for `DATABASE_URL`, `MIGRATOR_DATABASE_URL`, `REPORTING_API_TOKEN`, and the platform encryption secrets required by the target workloads.
3. Confirm the previous deploy metadata file is retained under `infra/cloud-run/.deploy-state/` before replacing a live environment.
4. Confirm production `ALERT_NOTIFICATION_CHANNELS` contains the Cloud Monitoring on-call notification channel resource names before applying monitoring.

For staged releases, prefer:

1. `sh infra/cloud-run/promote.sh staging`
2. Validate `sh infra/cloud-run/smoke-test.sh staging`
3. Confirm the smoke log shows `/api/reporting/meta-order-value` returning `401` without auth and succeeding with the reporting bearer token for the bounded `startDate` and `endDate` query
4. Confirm the Meta order-value scheduler is active in non-prod with `sh infra/cloud-run/scheduler.sh staging meta-order-value status`
5. After non-prod validation, `sh infra/cloud-run/promote.sh production`
6. Validate `sh infra/cloud-run/smoke-test.sh production` and retain the same Meta order value smoke evidence for production promotion records
7. Confirm the production scheduler with `sh infra/cloud-run/scheduler.sh production meta-order-value status`

Do not sign off staging or continue to production unless the smoke evidence includes the authenticated Meta order value response contract check.

## Meta Scheduler Controls

- `META_ADS_ORDER_VALUE_SCHEDULER_PAUSED` controls whether deploys leave the hourly Meta order-value scheduler active or paused.
- `META_ADS_ORDER_VALUE_SYNC_SCHEDULE` controls the Meta order-value Cloud Scheduler cron.
- `META_ADS_SCHEDULER_ATTEMPT_DEADLINE`, `META_ADS_SCHEDULER_MAX_RETRY_ATTEMPTS`, `META_ADS_SCHEDULER_MIN_BACKOFF`, `META_ADS_SCHEDULER_MAX_BACKOFF`, and `META_ADS_SCHEDULER_MAX_DOUBLINGS` control Cloud Scheduler retry behavior.
- `META_ADS_JOB_TIMEOUT_SECONDS` and `META_ADS_JOB_MAX_RETRIES` control the Cloud Run Job execution budget.
- `META_ADS_ORDER_VALUE_SYNC_ENABLED` is the emergency kill switch for Meta order-value extraction without disabling the broader deploy surface.
- `META_ADS_METADATA_SCHEDULER_NAME` and `GOOGLE_ADS_METADATA_SCHEDULER_NAME` identify the campaign metadata refresh schedulers created by deploys.
- `META_ADS_METADATA_REFRESH_REQUESTED_BY` and `GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY` should appear in `campaign_metadata_sync_job_lifecycle` logs for scheduler-triggered refreshes.
- `MMM_BASELINE_SCHEDULER_PAUSED` controls whether deploys leave the weekly MMM baseline scheduler active or paused.
- `MMM_BASELINE_SCHEDULE`, `MMM_BASELINE_TIME_ZONE`, `MMM_BASELINE_LOOKBACK_DAYS`, `MMM_BASELINE_LAG_DAYS`, and `MMM_BASELINE_ATTRIBUTION_MODEL` control the scheduled MMM training window and model anchor.
- `mmm_baseline_job_lifecycle` logs are the source for MMM failure and drift alerts.
- `MMM_BAYESIAN_FREEZE_ID` is required before running or enabling the scheduler for `bayesian_hierarchical_mmm_v1`; keep it empty until the approved freeze id is promoted as a release gate.
- `MMM_BAYESIAN_SCHEDULER_PAUSED`, `MMM_BAYESIAN_SCHEDULE`, `MMM_BAYESIAN_TIME_ZONE`, `MMM_BAYESIAN_LOOKBACK_DAYS`, `MMM_BAYESIAN_LAG_DAYS`, and `MMM_BAYESIAN_ATTRIBUTION_MODEL` control the separate Bayesian job.
- `MMM_BAYESIAN_JOB_CPU`, `MMM_BAYESIAN_JOB_MEMORY`, `MMM_BAYESIAN_JOB_TIMEOUT_SECONDS`, and `MMM_BAYESIAN_JOB_MAX_RETRIES` control Bayesian Cloud Run resources and retry behavior.
- `mmm_bayesian_job_lifecycle` logs are the source for Bayesian failure, stale success, diagnostics, and missing artifact alerts.

Recommended operating posture:

- `dev`: scheduler paused
- `staging`: scheduler active for hourly validation
- `production`: scheduler active only after staging validation passes

## Rollback And Toggle

1. If the issue is limited to Meta hourly ingestion, pause only the Meta scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-order-value pause`
2. If MMM calibration drift or model failures are active, pause the MMM scheduler while upstream freshness is repaired:
   `sh infra/cloud-run/scheduler.sh <environment> mmm-baseline pause`
   `sh infra/cloud-run/scheduler.sh <environment> mmm-bayesian pause`
3. If the scheduler should stay deployed but order-value extraction must stop, set `META_ADS_ORDER_VALUE_SYNC_ENABLED="false"` in the target environment file and rerun `sh infra/cloud-run/deploy.sh <environment>`.
4. If the service rollout itself must be reverted, use `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`.
5. If a schema change must be reversed, apply the matching manual rollback file from `db/rollbacks/` with the migrator database credentials, then rerun the smoke test before resuming schedulers.
6. After remediation, resume the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-order-value resume`
7. For MMM, manually execute one successful baseline job and then resume:
   `sh infra/cloud-run/scheduler.sh <environment> mmm-baseline resume`
   For Bayesian MMM, manually execute one successful `roas-radar-mmm-bayesian-<environment>` job with a promoted `MMM_BAYESIAN_FREEZE_ID`, then resume `sh infra/cloud-run/scheduler.sh <environment> mmm-bayesian resume`.

For upstream metadata quota incidents, pause the affected campaign metadata scheduler with `gcloud scheduler jobs pause`, then use `gcloud scheduler jobs resume` after `campaign_metadata_sync_job_lifecycle` logs show successful manual or scheduler refreshes.

## Promotion Evidence

Attach these artifacts to the release record before production sign-off:

- Terraform plan output for staging and production.
- Cloud Build URL and image tag promoted by `cloudbuild.release.yaml`.
- Migration job execution id and final status for staging and production.
- Smoke-test output for staging and production.
- Staging rollback drill output when `RUN_STAGING_ROLLBACK_DRILL=true`.
- Scheduler status for Meta order-value, attribution materialization, identity graph backfill, retention, and data quality jobs.
- Latest `mmm_model_runs` row for the baseline model after the MMM scheduler or manual job run.
- Latest `mmm_model_runs` row for `bayesian_hierarchical_mmm_v1` includes `approved_freeze_id`, posterior diagnostics, output artifacts, and calibration status after a Bayesian scheduler or manual run.
- Monitoring apply output from `npm run ops:monitoring:apply -- <environment>` showing log metrics, alert policies, and the SLO dashboard were updated.
