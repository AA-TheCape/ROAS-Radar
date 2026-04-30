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

1. `sh infra/cloud-run/promote.sh staging`
2. Validate `sh infra/cloud-run/smoke-test.sh staging`
3. Confirm the Meta scheduler is active in non-prod with `sh infra/cloud-run/scheduler.sh staging meta-ads status`
4. After non-prod validation, `sh infra/cloud-run/promote.sh production`
5. Confirm the production scheduler with `sh infra/cloud-run/scheduler.sh production meta-ads status`

## Meta Scheduler Controls

- `META_ADS_SCHEDULER_PAUSED` controls whether deploys leave the hourly scheduler active or paused.
- `META_ADS_SCHEDULER_ATTEMPT_DEADLINE`, `META_ADS_SCHEDULER_MAX_RETRY_ATTEMPTS`, `META_ADS_SCHEDULER_MIN_BACKOFF`, `META_ADS_SCHEDULER_MAX_BACKOFF`, and `META_ADS_SCHEDULER_MAX_DOUBLINGS` control Cloud Scheduler retry behavior.
- `META_ADS_JOB_TIMEOUT_SECONDS` and `META_ADS_JOB_MAX_RETRIES` control the Cloud Run Job execution budget.
- `META_ADS_ORDER_VALUE_SYNC_ENABLED` is the emergency kill switch for Meta order-value extraction without disabling the broader deploy surface.

Recommended operating posture:

- `dev`: scheduler paused
- `staging`: scheduler active for hourly validation
- `production`: scheduler active only after staging validation passes

## Rollback And Toggle

1. If the issue is limited to Meta hourly ingestion, pause only the Meta scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-ads pause`
2. If the scheduler should stay deployed but order-value extraction must stop, set `META_ADS_ORDER_VALUE_SYNC_ENABLED="false"` in the target environment file and rerun `sh infra/cloud-run/deploy.sh <environment>`.
3. If the service rollout itself must be reverted, use `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`.
4. After remediation, resume the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-ads resume`
