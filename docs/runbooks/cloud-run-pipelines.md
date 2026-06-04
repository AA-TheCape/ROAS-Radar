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
  - `roas-radar-meta-deterministic-sync`
  - `roas-radar-meta-ads-metadata-refresh`
  - `roas-radar-google-ads-metadata-refresh`
  - `roas-radar-google-ads-sync`
  - `roas-radar-ga4-ingestion`
  - `roas-radar-campaign-metadata-backfill`
  - `roas-radar-shopify-order-reimport`
  - `roas-radar-order-attribution-backfill`
  - `roas-radar-shopify-attribution-recovery`
  - `roas-radar-ga4-fallback-recovery`
  - `roas-radar-dead-letter-replay`
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
5. Confirm deterministic view/impression ingestion is scheduled with `sh infra/cloud-run/scheduler.sh staging meta-deterministic status`
6. After non-prod validation, `sh infra/cloud-run/promote.sh production`
7. Validate `sh infra/cloud-run/smoke-test.sh production` and retain the same Meta order value smoke evidence for production promotion records
8. Confirm the production schedulers with `sh infra/cloud-run/scheduler.sh production meta-order-value status` and `sh infra/cloud-run/scheduler.sh production meta-deterministic status`

Do not sign off staging or continue to production unless the smoke evidence includes the authenticated Meta order value response contract check.

## Meta Scheduler Controls

- `META_ADS_ORDER_VALUE_SCHEDULER_PAUSED` controls whether deploys leave the hourly Meta order-value scheduler active or paused.
- `META_ADS_ORDER_VALUE_SYNC_SCHEDULE` controls the Meta order-value Cloud Scheduler cron.
- `META_ADS_DETERMINISTIC_SCHEDULER_PAUSED` controls whether deploys leave the Meta deterministic view/impression scheduler active or paused.
- `META_ADS_DETERMINISTIC_SYNC_SCHEDULE` controls the deterministic Cloud Scheduler cron.
- `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS` and `META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS` control first-run and steady-state backfill windows.
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
2. If deterministic view/impression extraction is producing bad data, pause that scheduler independently:
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic pause`
3. If MMM calibration drift or model failures are active, pause the MMM scheduler while upstream freshness is repaired:
   `sh infra/cloud-run/scheduler.sh <environment> mmm-baseline pause`
   `sh infra/cloud-run/scheduler.sh <environment> mmm-bayesian pause`
4. If the scheduler should stay deployed but order-value extraction must stop, set `META_ADS_ORDER_VALUE_SYNC_ENABLED="false"` in the target environment file and rerun `sh infra/cloud-run/deploy.sh <environment>`.
5. If deterministic extraction must stop but the job should remain deployed, set `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` and rerun `sh infra/cloud-run/deploy.sh <environment>`.
6. If the service rollout itself must be reverted, use `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`.
7. If a schema change must be reversed, apply the matching manual rollback file from `db/rollbacks/` with the migrator database credentials, then rerun the smoke test before resuming schedulers.
8. After remediation, resume the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-order-value resume`
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic resume`
9. For MMM, manually execute one successful baseline job and then resume:
   `sh infra/cloud-run/scheduler.sh <environment> mmm-baseline resume`
   For Bayesian MMM, manually execute one successful `roas-radar-mmm-bayesian-<environment>` job with a promoted `MMM_BAYESIAN_FREEZE_ID`, then resume `sh infra/cloud-run/scheduler.sh <environment> mmm-bayesian resume`.

For upstream metadata quota incidents, pause the affected campaign metadata scheduler with `gcloud scheduler jobs pause`, then use `gcloud scheduler jobs resume` after `campaign_metadata_sync_job_lifecycle` logs show successful manual or scheduler refreshes.

## Recovery Queue Operations

Automatic recovery queue checks:

1. Confirm the queued run is bounded by job type, scope key, and date range:
   `gcloud logging read 'jsonPayload.message="recovery_job_enqueued" AND jsonPayload.jobType="<job-type>"' --project=<project> --limit=20 --format=json`
2. Confirm duplicate alert or scheduler signals reused the existing idempotency key instead of creating overlapping work.
3. Confirm the completion report includes `sourcePrecedence=["shopify","ga4","ad_platforms"]`, dry-run state, counters, artifacts, and per-record failures with `retryable=true` or `retryable=false`.
4. For stale `running` jobs, execute the recovery worker once, then verify heartbeat-expired runs either returned to `queued` with backoff or moved to `dead_lettered` after max attempts.

Dead-letter replay workflow:

1. Find the failed source and window:
   `gcloud logging read 'jsonPayload.event="recovery_record_failure" AND jsonPayload.alertable=true' --project=<project> --limit=50 --format=json`
2. Run the matching replay command with `--dry-run` first.
3. Verify `candidateCount` and `dryRunCount` match the intended records, and verify source records were not requeued.
4. After the upstream issue is fixed, rerun without `--dry-run`; verify `replayedCount` increases, source rows return to queued or pending state, and dead letters are marked replayed.
5. If replaying Shopify recovery, confirm raw payload hashes and storage URIs remain unchanged. Replay must reprocess the preserved payload, not fetch a fresh replacement unless the job explicitly documents reimport behavior.

Failure triage:

- Retryable: upstream timeout, quota, lock timeout, heartbeat expiration before max attempts, or temporarily unavailable GA4/ad-platform export. These should return to `queued` with backoff.
- Permanent: invalid schema, unsupported job type, missing immutable source identifiers, malformed preserved raw payload, or exhausted retry attempts. These should be `dead_lettered` with enough payload context to replay after operator correction.
- Source precedence: Shopify fields win over GA4 fields; GA4 fills only missing Shopify fields; ad-platform metadata refreshes names and hierarchy only.

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
