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
- Cloud Scheduler:
  - one scheduler per recurring Cloud Run Job

## Pre-deploy checks

Run the backend verification contract from a clean Node 22 checkout in this order:

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

## Confidence Scoring Rollout

Use this sequence for the confidence-scoring schema and service release. The migration is additive for deployed services: old revisions continue to read the legacy attribution columns and snapshots while the new revision writes and exposes `confidenceScore`, `attributionSource`, `matchingMethod`, and `lastAttributionRunAt`.

1. Confirm `npm run db:migrate:check`, `npm run test:attribution`, and `npm --prefix dashboard run build` pass against the release image source.
2. Deploy staging through `RUN_STAGING_ROLLBACK_DRILL=true sh infra/cloud-run/promote.sh staging`.
3. Keep compatibility mode during transition by leaving the previous API, worker, and dashboard revisions available in the deploy metadata. Do not run rollback SQL during this phase; the schema expansion is intentionally backward-compatible.
4. Validate staging smoke output includes `/api/reporting/orders` and confirms bounded `confidenceScore` values plus `attributionSource`, `matchingMethod`, and `lastAttributionRunAt` keys when rows are present.
5. Validate the dashboard order table renders and can sort by Confidence for the same bounded date window.
6. Promote production with `RUN_STAGING_ROLLBACK_DRILL=true sh infra/cloud-run/promote.sh production` so production promotion is blocked unless the staging rollback drill has already succeeded.
7. Re-run `sh infra/cloud-run/smoke-test.sh production` and retain the production smoke output as the post-deploy API evidence.
8. For 24 hours after production deployment, monitor `roas_order_attribution_confidence_backfill_progress`, API latency, and attribution worker backlog before treating the compatibility window as closed.

Rollback path:

1. If the confidence API/UI check fails but `/readyz` is healthy, route services back to the previous revisions with `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`.
2. Re-run `sh infra/cloud-run/smoke-test.sh <environment>` after rollback. The smoke helper remains valid because previous revisions are compatible with the expanded schema.
3. Pause only the order-attribution materialization scheduler if confidence writes are producing bad metadata while other services remain healthy: `gcloud scheduler jobs pause <order-attribution-materialization-scheduler> --project=<project> --location=<region>`.
4. Use `db/rollbacks/0046_add_order_attribution_confidence_metadata.down.sql` only after traffic is pinned to a revision that does not reference the new columns and after confirming no new revision, worker, or job uses confidence metadata.

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

## Manual Backfill And Recovery

Manual recovery jobs are deployed with `--max-retries=0` and job-level invoker grants only. By default the environment deployer service account and attribution worker service account can execute them; add human or break-glass identities through `MANUAL_JOB_INVOKER_MEMBERS` as comma-separated IAM members, such as `group:roas-radar-operators@example.com`.

Use dry runs first for every recovery workflow. The application-level recovery queue owns retry classification and dead-letter state; Cloud Run Job retries stay disabled so a failed execution does not duplicate side effects outside the idempotency key.

Use `gcloud run jobs execute` with a complete arg override. Examples:

```sh
gcloud run jobs execute roas-radar-order-attribution-backfill-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,attribution:backfill-orders:start,--,--from,2026-05-01T00:00:00Z,--to,2026-05-02T00:00:00Z,--requested-by,operator@example.com,--dry-run \
  --wait
```

```sh
gcloud run jobs execute roas-radar-campaign-metadata-backfill-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,campaign-metadata:backfill:start,--,--mode,api-refresh,--requested-by,operator@example.com,--platforms,meta_ads,--dry-run \
  --wait
```

Before running production jobs, confirm the target job service account has only the required Secret Manager bindings in `infra/cloud-run/bootstrap-iam.sh`, then run the same command in staging and retain the execution log URL.

Automatic recovery queue checks:

1. Confirm the queued run is bounded by job type, scope key, and date range:
   `gcloud logging read 'jsonPayload.message="recovery_job_enqueued" AND jsonPayload.jobType="<job-type>"' --project=<project> --limit=20 --format=json`
2. Confirm duplicate alert or scheduler signals reused the existing idempotency key instead of creating overlapping work.
3. Confirm the completion report includes `sourcePrecedence=["shopify","ga4","ad_platforms"]`, dry-run state, counters, artifacts, and per-record failures with `retryable=true` or `retryable=false`.
4. For stale `running` jobs, execute the recovery worker once, then verify heartbeat-expired runs either returned to `queued` with backoff or moved to `dead_lettered` after max attempts.

Dead-letter replay workflow:

1. Find the failed source and window:
   `gcloud logging read 'jsonPayload.sourceTable="recovery_job_runs" OR jsonPayload.sourceTable="attribution_jobs"' --project=<project> --limit=50 --format=json`
2. Run replay in dry-run mode first:
   `gcloud run jobs execute roas-radar-dead-letter-replay-<environment> --project=<project> --region=us-central1 --args=run,dead-letters:replay:start,--,--source-table,recovery_job_runs,--from,2026-05-01T00:00:00Z,--to,2026-05-02T00:00:00Z,--requested-by,operator@example.com,--dry-run --wait`
3. Verify `candidateCount` and `dryRunCount` match the intended records, and verify source records were not requeued.
4. After the upstream issue is fixed, rerun without `--dry-run`; verify `replayedCount` increases, source rows return to queued or pending state, and dead letters are marked replayed.
5. If replaying Shopify recovery, confirm raw payload hashes and storage URIs remain unchanged. Replay must reprocess the preserved payload, not fetch a fresh replacement unless the job explicitly documents reimport behavior.

Failure triage:

- Retryable: upstream timeout, quota, lock timeout, heartbeat expiration before max attempts, or temporarily unavailable GA4/ad-platform export. These should return to `queued` with backoff.
- Permanent: invalid schema, unsupported job type, missing immutable source identifiers, malformed preserved raw payload, or exhausted retry attempts. These should be `dead_lettered` with enough payload context to replay after operator correction.
- Source precedence: Shopify fields win over GA4 fields; GA4 fills only missing Shopify fields; ad-platform metadata refreshes names and hierarchy only.

Recommended operating posture:

- `dev`: scheduler paused
- `staging`: scheduler active for hourly validation
- `production`: scheduler active only after staging validation passes

## Rollback And Toggle

1. If the issue is limited to Meta hourly ingestion, pause only the Meta scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-order-value pause`
2. If deterministic view/impression ingestion is the issue, pause only that scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic pause`
3. If the scheduler should stay deployed but order-value extraction must stop, set `META_ADS_ORDER_VALUE_SYNC_ENABLED="false"` in the target environment file and rerun `sh infra/cloud-run/deploy.sh <environment>`.
4. If deterministic extraction must stop but the job should remain deployed, set `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` and rerun `sh infra/cloud-run/deploy.sh <environment>`.
5. If the service rollout itself must be reverted, use `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`.
6. After remediation, resume the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-order-value resume`

For upstream metadata quota incidents, pause the affected campaign metadata scheduler with `gcloud scheduler jobs pause`, then use `gcloud scheduler jobs resume` after `campaign_metadata_sync_job_lifecycle` logs show successful manual or scheduler refreshes.
