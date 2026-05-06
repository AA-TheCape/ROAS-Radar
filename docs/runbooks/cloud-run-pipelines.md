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
- Cloud Scheduler:
  - one scheduler per recurring Cloud Run Job

## Pre-deploy checks

Run the backend verification contract from a clean Node 22 checkout in this order:

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

Recommended operating posture:

- `dev`: scheduler paused
- `staging`: scheduler active for hourly validation
- `production`: scheduler active only after staging validation passes

## Rollback And Toggle

1. If the issue is limited to Meta hourly ingestion, pause only the Meta scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-order-value pause`
2. If the scheduler should stay deployed but order-value extraction must stop, set `META_ADS_ORDER_VALUE_SYNC_ENABLED="false"` in the target environment file and rerun `sh infra/cloud-run/deploy.sh <environment>`.
3. If the service rollout itself must be reverted, use `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`.
4. After remediation, resume the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-order-value resume`

For upstream metadata quota incidents, pause the affected campaign metadata scheduler with `gcloud scheduler jobs pause`, then use `gcloud scheduler jobs resume` after `campaign_metadata_sync_job_lifecycle` logs show successful manual or scheduler refreshes.
