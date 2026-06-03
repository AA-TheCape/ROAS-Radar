# Cloud Run Deployment

This directory contains the checked-in deployment contract for the Node backend, the optional dashboard service, and the scheduled jobs that support attribution and ad-ingestion workloads.

The root backend `Dockerfile` is the production packaging path for every backend Cloud Run workload in this directory. It builds on `node:22-bookworm-slim` and defaults the API container command to `npm run start:api`.

The deployment flow assumes twenty deployable workloads plus ten Cloud Scheduler triggers:

- `roas-radar-api`: public Cloud Run service for `/track`, Shopify webhooks, and authenticated reporting APIs.
- `roas-radar-dashboard`: public Cloud Run service for the React reporting dashboard.
- `roas-radar-attribution-worker`: internal Cloud Run service for attribution and asynchronous processing.
- `roas-radar-migrate`: Cloud Run Job that runs `npm run db:migrate:start` with elevated database credentials.
- `roas-radar-meta-ads-sync`: Cloud Run Job that runs `npm run meta-ads:sync:start` once per invocation.
- `roas-radar-meta-order-value-sync`: Cloud Run Job that runs `npm run meta-ads:order-value:start` once per invocation.
- `roas-radar-meta-deterministic-sync`: Cloud Run Job that runs `npm run meta-ads:deterministic:start` once per invocation.
- `roas-radar-google-ads-sync`: Cloud Run Job that runs `npm run google-ads:sync:start` once per invocation.
- `roas-radar-ga4-ingestion`: Cloud Run Job that runs `npm run ga4:ingest:start` once per invocation.
- `roas-radar-campaign-metadata-backfill`: manual Cloud Run Job for `npm run campaign-metadata:backfill:start`.
- `roas-radar-shopify-order-reimport`: manual Cloud Run Job for `npm run shopify:reimport-orders:start`.
- `roas-radar-order-attribution-backfill`: manual Cloud Run Job for `npm run attribution:backfill-orders:start`.
- `roas-radar-shopify-attribution-recovery`: manual Cloud Run Job for `npm run attribution:recover-shopify-hints:start`.
- `roas-radar-ga4-fallback-recovery`: manual Cloud Run Job for `npm run attribution:recover-ga4-fallback:start`.
- `roas-radar-dead-letter-replay`: manual Cloud Run Job for `npm run dead-letters:replay:start`.
- `roas-radar-session-retention`: Cloud Run Job that runs `npm run session-attribution:retention:start` to prune expired attribution-session records.
- `roas-radar-attribution-qa-retention`: Cloud Run Job that runs `npm run attribution-qa:retention:start` to prune expired Attribution QA raw evidence and embedded QA snapshots.
- `roas-radar-data-quality`: Cloud Run Job that runs `npm run data-quality:check:start` once per invocation.
- `roas-radar-identity-graph-backfill`: Cloud Run Job that runs `npm run identity:backfill-graph:start` over a recent window to reconcile graph attachments and catch missed identity stitching.
- `roas-radar-order-attribution-materialization`: Cloud Run Job that runs `npm run attribution:materialization:start` over a recent order window to recover attribution and refresh reporting aggregates.
- `roas-radar-meta-ads-sync-scheduler`: Cloud Scheduler job that invokes the Meta Ads Cloud Run Job.
- `roas-radar-meta-order-value-sync-scheduler`: Cloud Scheduler job that invokes the Meta order-value Cloud Run Job.
- `roas-radar-meta-deterministic-sync-scheduler`: Cloud Scheduler job that invokes the Meta deterministic view/impression Cloud Run Job.
- `roas-radar-google-ads-sync-scheduler`: Cloud Scheduler job that invokes the Google Ads Cloud Run Job.
- `roas-radar-ga4-ingestion-scheduler`: Cloud Scheduler job that invokes the GA4 ingestion Cloud Run Job.
- `roas-radar-session-retention-scheduler`: Cloud Scheduler job that invokes the session-retention Cloud Run Job.
- `roas-radar-attribution-qa-retention-scheduler`: Cloud Scheduler job that invokes the Attribution QA retention Cloud Run Job daily.
- `roas-radar-data-quality-scheduler`: Cloud Scheduler job that invokes the data-quality Cloud Run Job.
- `roas-radar-identity-graph-backfill-scheduler`: Cloud Scheduler job that invokes the identity-graph backfill Cloud Run Job.
- `roas-radar-order-attribution-materialization-scheduler`: Cloud Scheduler job that invokes the order-attribution materialization Cloud Run Job.

## Files

Each recurring and manual job now runs as its own service account. Cloud Scheduler uses a dedicated invoker identity, and manual backfill/recovery jobs grant job-level `roles/run.invoker` only to the attribution worker, the environment deployer service account, and any `MANUAL_JOB_INVOKER_MEMBERS` entries. That keeps ads sync, GA4 ingestion, data quality, identity reconciliation, attribution materialization, and operator recovery paths from sharing unnecessary secret access.

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

- `TRACKING_ALLOWED_ORIGINS`
- `API_JSON_BODY_LIMIT`
- `TRACKING_BODY_LIMIT`
- `SHOPIFY_WEBHOOK_BODY_LIMIT`
- `SHOPIFY_APP_BASE_URL`
- `SHOPIFY_APP_API_VERSION`
- `SHOPIFY_APP_SCOPES`
- `SHOPIFY_APP_POST_INSTALL_REDIRECT_URL`
- `META_ADS_APP_ID`
- `META_ADS_APP_BASE_URL`
- `META_ADS_APP_SCOPES`
- `META_ADS_AD_ACCOUNT_ID`
- `META_ADS_API_VERSION`
- `META_ADS_METADATA_ACCESS_TOKEN_SECRET_NAME`
- `DASHBOARD_API_BASE_URL`
- `API_CPU`
- `API_MEMORY`
- `API_CONCURRENCY`
- `API_TIMEOUT_SECONDS`
- `WORKER_CPU`
- `WORKER_MEMORY`
- `WORKER_CONCURRENCY`
- `WORKER_TIMEOUT_SECONDS`
- `META_ADS_JOB_NAME`
- `META_ADS_ORDER_VALUE_JOB_NAME`
- `META_ADS_DETERMINISTIC_JOB_NAME`
- `GOOGLE_ADS_JOB_NAME`
- `GA4_INGESTION_JOB_NAME`
- `CAMPAIGN_METADATA_BACKFILL_JOB_NAME`
- `SHOPIFY_ORDER_REIMPORT_JOB_NAME`
- `ORDER_ATTRIBUTION_BACKFILL_JOB_NAME`
- `SHOPIFY_ATTRIBUTION_RECOVERY_JOB_NAME`
- `GA4_FALLBACK_RECOVERY_JOB_NAME`
- `DEAD_LETTER_REPLAY_JOB_NAME`
- `META_ADS_SCHEDULER_JOB_NAME`
- `META_ADS_ORDER_VALUE_SCHEDULER_JOB_NAME`
- `META_ADS_DETERMINISTIC_SCHEDULER_JOB_NAME`
- `GOOGLE_ADS_SCHEDULER_JOB_NAME`
- `GA4_INGESTION_SCHEDULER_JOB_NAME`
- `RETENTION_JOB_NAME`
- `ATTRIBUTION_QA_RETENTION_JOB_NAME`
- `DATA_QUALITY_JOB_NAME`
- `IDENTITY_GRAPH_BACKFILL_JOB_NAME`
- `ORDER_ATTRIBUTION_MATERIALIZATION_JOB_NAME`
- `RETENTION_SCHEDULER_JOB_NAME`
- `ATTRIBUTION_QA_RETENTION_SCHEDULER_JOB_NAME`
- `DATA_QUALITY_SCHEDULER_JOB_NAME`
- `IDENTITY_GRAPH_BACKFILL_SCHEDULER_JOB_NAME`
- `ORDER_ATTRIBUTION_MATERIALIZATION_SCHEDULER_JOB_NAME`
- `RETENTION_JOB_SERVICE_ACCOUNT_NAME`
- `META_ADS_JOB_SERVICE_ACCOUNT_NAME`
- `META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME`
- `GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME`
- `GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME`
- `CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME`
- `SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME`
- `ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_NAME`
- `SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_NAME`
- `GA4_FALLBACK_RECOVERY_JOB_SERVICE_ACCOUNT_NAME`
- `DEAD_LETTER_REPLAY_JOB_SERVICE_ACCOUNT_NAME`
- `DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME`
- `IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME`
- `ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME`
- `SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME`
- `DEPLOYER_SERVICE_ACCOUNT_NAME`
- `MANUAL_JOB_INVOKER_MEMBERS`
- `ADS_SYNC_DATABASE_POOL_MAX`
- `ADS_SYNC_TIME_ZONE`
- `META_ADS_SYNC_SCHEDULE`
- `META_ADS_ORDER_VALUE_SYNC_SCHEDULE`
- `META_ADS_DETERMINISTIC_SYNC_SCHEDULE`
- `META_ADS_DETERMINISTIC_SCHEDULER_PAUSED`
- `META_ADS_DETERMINISTIC_SYNC_ENABLED`
- `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS`
- `META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS`
- `META_ADS_ORDER_VALUE_SCHEDULER_PAUSED`
- `META_ADS_SCHEDULER_PAUSED`
- `META_ADS_SCHEDULER_ATTEMPT_DEADLINE`
- `META_ADS_SCHEDULER_MAX_RETRY_ATTEMPTS`
- `META_ADS_SCHEDULER_MIN_BACKOFF`
- `META_ADS_SCHEDULER_MAX_BACKOFF`
- `META_ADS_SCHEDULER_MAX_DOUBLINGS`
- `META_ADS_JOB_TIMEOUT_SECONDS`
- `META_ADS_JOB_MAX_RETRIES`
- `META_ADS_ORDER_VALUE_SYNC_ENABLED`
- `META_ADS_ORDER_VALUE_SYNC_INTERVAL_MS`
- `META_ADS_ORDER_VALUE_WINDOW_DAYS`
- `META_ADS_ORDER_VALUE_ANOMALY_MIN_ROWS`
- `META_ADS_ORDER_VALUE_NULL_SPIKE_MIN_RATIO`
- `META_ADS_ORDER_VALUE_NULL_SPIKE_RATIO_DELTA`
- `GOOGLE_ADS_SYNC_SCHEDULE`
- `GA4_INGESTION_SCHEDULE`
- `GA4_INGESTION_SCHEDULER_TIME_ZONE`
- `GA4_INGESTION_SCHEDULER_ENABLED`
- `GA4_INGESTION_REQUESTED_BY`
- `GA4_INGESTION_BATCH_SIZE`
- `GA4_INGESTION_MAX_RETRIES`
- `GA4_INGESTION_INITIAL_BACKOFF_SECONDS`
- `GA4_INGESTION_MAX_BACKOFF_SECONDS`
- `GA4_INGESTION_STALE_LOCK_MINUTES`
- `GA4_BIGQUERY_PROJECT_ID`
- `GA4_BIGQUERY_LOCATION`
- `GA4_BIGQUERY_DATASET`
- `GA4_BIGQUERY_EVENTS_TABLE_PATTERN`
- `GA4_BIGQUERY_INTRADAY_TABLE_PATTERN`
- `GA4_BIGQUERY_LOOKBACK_HOURS`
- `GA4_BIGQUERY_BACKFILL_HOURS`
- `GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID`
- `GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION`
- `GOOGLE_ADS_TRANSFER_BIGQUERY_DATASET`
- `GOOGLE_ADS_TRANSFER_TABLE_PATTERN`
- `GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS`
- `GA4_LINKED_GOOGLE_ADS_CUSTOMER_IDS`
- `RETENTION_SCHEDULE`
- `ATTRIBUTION_QA_RETENTION_SCHEDULE`
- `DATA_QUALITY_SCHEDULE`
- `IDENTITY_GRAPH_BACKFILL_SCHEDULE`
- `ORDER_ATTRIBUTION_MATERIALIZATION_SCHEDULE`
- `IDENTITY_GRAPH_BACKFILL_REQUESTED_BY`
- `IDENTITY_GRAPH_BACKFILL_LOOKBACK_DAYS`
- `IDENTITY_GRAPH_BACKFILL_LAG_HOURS`
- `IDENTITY_GRAPH_BACKFILL_BATCH_SIZE`
- `IDENTITY_GRAPH_BACKFILL_MAX_BATCHES`
- `IDENTITY_GRAPH_BACKFILL_SOURCES`
- `ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY`
- `ORDER_ATTRIBUTION_MATERIALIZATION_LOOKBACK_DAYS`
- `ORDER_ATTRIBUTION_MATERIALIZATION_LAG_DAYS`
- `ORDER_ATTRIBUTION_MATERIALIZATION_LIMIT`
- `ORDER_ATTRIBUTION_MATERIALIZATION_DRY_RUN`
- `ORDER_ATTRIBUTION_MATERIALIZATION_ONLY_WEB_ORDERS`
- `ORDER_ATTRIBUTION_MATERIALIZATION_SKIP_SHOPIFY_WRITEBACK`
- `DATA_QUALITY_TARGET_LAG_DAYS`
- `DATA_QUALITY_ANOMALY_LOOKBACK_DAYS`
- `DATA_QUALITY_ANOMALY_THRESHOLD_RATIO`
- `DATA_QUALITY_ANOMALY_MIN_BASELINE`
- `DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD`
- `DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD`
- `DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD`
- `DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD`
- `DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD`
- `DATA_QUALITY_SAMPLE_LIMIT`
- `SESSION_ATTRIBUTION_RETENTION_DAYS`
- `SESSION_ATTRIBUTION_RETENTION_BATCH_SIZE`
- `SESSION_ATTRIBUTION_RETENTION_MAX_BATCHES`
- `ATTRIBUTION_QA_RETENTION_DAYS`
- `ATTRIBUTION_QA_RETENTION_BATCH_SIZE`
- `ATTRIBUTION_QA_RETENTION_MAX_BATCHES`

Meta deployment blockers:

- `MIGRATOR_DATABASE_URL` must exist in Secret Manager before deploy because the migrator job runs before service rollout by default.
- `META_ADS_APP_SECRET` and `META_ADS_ENCRYPTION_KEY` must exist in Secret Manager before `bootstrap-iam.sh` or `deploy.sh` can grant/bind them. `META_ADS_METADATA_ACCESS_TOKEN_SECRET_NAME` is optional; leave it empty to use encrypted Meta connection tokens from application storage.
- `META_ADS_APP_ID`, `META_ADS_AD_ACCOUNT_ID`, `META_ADS_APP_SCOPES`, and `META_ADS_API_VERSION` must be populated in the environment file before enabling Meta sync or metadata schedulers.
- `CLOUD_SQL_CONNECTION_NAME` must point at the environment Cloud SQL instance; API, worker, migrator, Meta sync, metadata refresh, and campaign metadata backfill workloads all attach it for the metadata cache table.

Run these commands from the repo root on Node 22 before deploying:

1. Provision Cloud SQL and private networking from `infra/cloud-sql/`.
2. Run `infra/cloud-run/bootstrap-iam.sh ENVIRONMENT` to create service accounts and grant IAM roles.
3. Create the environment secrets in Secret Manager.
4. Populate `infra/cloud-run/environments/dev.env`, `staging.env`, and `production.env`.
5. Deploy with `infra/cloud-run/deploy.sh ENVIRONMENT`.
6. Apply monitoring with `infra/monitoring/apply.sh ENVIRONMENT`.
7. Validate the scheduled jobs and schedulers with `docs/runbooks/cloud-run-pipelines.md`.

This sequence validates the compiled backend entrypoints, the migration check, the GA4-critical test suite, and the Docker packaging path that Cloud Run consumes.

For one-off environment deploys, run `infra/cloud-run/deploy.sh dev`, `infra/cloud-run/deploy.sh staging`, or `infra/cloud-run/deploy.sh production`.

`infra/cloud-run/deploy.sh` now deploys the migrator job first, runs schema migration before any service rollout, then deploys the API, worker, dashboard, jobs, and schedulers. If `DEPLOY_METADATA_FILE` is set, the script also records the previous and newly deployed Cloud Run revisions for the API, worker, and dashboard so rollback can route traffic back to the prior revision quickly.

For staged promotion, use `infra/cloud-run/promote.sh <dev|staging|production>`. The promotion script:

- deploys each environment in order (`dev -> staging -> production`)
- runs migrations before shifting service traffic in each environment
- executes smoke tests after each environment deploy
- can run a staging rollback drill by toggling `RUN_STAGING_ROLLBACK_DRILL=true`
- persists rollout metadata in `infra/cloud-run/.deploy-state/`

- `SKIP_BUILDS=true`: reuse an already-pushed image tag instead of building locally
- `SHORT_SHA=<tag>` or `IMAGE_TAG=<tag>`: force the image tag that `deploy.sh` references
- `RUN_MIGRATIONS_ON_DEPLOY=true`: execute the migration job after the job definition is deployed and before API, worker, dashboard, and sync jobs are rolled out
- `APPLY_MONITORING_ON_DEPLOY=true`: apply the monitoring assets in `infra/monitoring/`

- `roas-radar-deployer-dev@<project>.iam.gserviceaccount.com` for dev
- `roas-radar-deployer-staging@<project>.iam.gserviceaccount.com` for staging
- `roas-radar-deployer-prod@<project>.iam.gserviceaccount.com` for production

The deploy contract for GA4 is:

1. Deploy the backend image that contains `dist/src/ga4-session-attribution-worker.js`.
2. Deploy the Cloud Run Job with command `npm` and args `run,ga4:ingest:start`.
3. Bind `GA4_BIGQUERY_ENABLED=true` plus `GA4_INGESTION_REQUESTED_BY`, batch size, retry, backoff, and stale-lock env vars.
4. Upsert the Cloud Scheduler job on `5 * * * *` unless the environment file says otherwise.
5. Keep the Cloud Run Job retry count at `0`; the application owns hour-level retry and dead-letter behavior.

Use `cloudbuild.release.yaml` for the full staged promotion pipeline. Its defaults promote through production, run migrations, apply monitoring, and require a successful staging rollback drill before production deployment continues. Use substitutions to stop at staging or disable the drill when needed.

## Confidence Scoring Release

Confidence scoring rolls out through the same expand/migrate/contract posture as the rest of the Cloud Run pipeline:

1. Expand: deploy and execute `roas-radar-migrate` before traffic moves. Migration `0046_add_order_attribution_confidence_metadata.sql` adds nullable confidence lookup columns, defaults for future writes, and `NOT VALID` constraints without historical backfill, validation, `NOT NULL`, or large index creation.
2. Migrate: deploy the API, worker, dashboard, and jobs from the same image tag. The deployed services write confidence metadata while legacy fields and attribution snapshots remain populated for old revisions.
3. Backfill: run `npm run attribution:backfill-confidence -- --dry-run --batch-size 1000`, then the write-enabled pass. Resume with `--resume-after-order-row-id <cursor>` if interrupted.
4. Index: run `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/operations/0046_add_order_attribution_confidence_indexes.sql` so Cloud SQL builds the large indexes concurrently outside the transactional migration runner.
5. Compatibility window: keep the deploy metadata file generated by `DEPLOY_METADATA_FILE` and do not run rollback SQL. The previous Cloud Run revisions can be restored with `rollback.sh` because the expanded schema is backward-compatible.
6. Contract: after backfill completion, smoke tests, dashboard verification, and at least one monitoring window are clean, run `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/operations/0046_contract_order_attribution_confidence_metadata.sql` to validate constraints and apply `NOT NULL`. Removing legacy fields requires a separate migration and release note.

The production smoke helper now validates `/api/reporting/orders` by default. It confirms rows expose `confidenceScore`, `attributionSource`, `matchingMethod`, and `lastAttributionRunAt`, and that confidence scores are either null or in the `0..1` range. Set `SMOKE_TEST_VALIDATE_CONFIDENCE=false` only for emergency diagnosis; do not use that override for release sign-off.

## Scheduled Pipelines

- `GA4_INGESTION_REQUESTED_BY=cloud-run-scheduler-<environment>`
- `GA4_INGESTION_BATCH_SIZE=6`
- `GA4_INGESTION_MAX_RETRIES=5`
- `GA4_INGESTION_INITIAL_BACKOFF_SECONDS=300`
- `GA4_INGESTION_MAX_BACKOFF_SECONDS=21600`
- `GA4_INGESTION_STALE_LOCK_MINUTES=30`
- `GA4_INGESTION_SCHEDULER_TIME_ZONE=Etc/UTC`

## Verification after deploy

Meta Ads, Meta order-value, and Google Ads sync remain hourly schedulers. If one scheduled job is unhealthy, pause only that scheduler entry and leave the attribution worker service running because it also drains the live attribution queue.

For Meta order-value specifically:

- `staging.env` is configured active (`META_ADS_ORDER_VALUE_SCHEDULER_PAUSED="false"`) for non-prod validation.
- `production.env` is configured active after the same deploy path is promoted through staging.
- `dev.env` is configured paused by default so hourly sync is not running continuously in the sandbox environment.
- scheduler retries are disabled at the Cloud Scheduler layer (`META_ADS_SCHEDULER_MAX_RETRY_ATTEMPTS="0"`) to avoid duplicate invocations; the Cloud Run Job owns the single retry budget via `META_ADS_JOB_MAX_RETRIES`.
- the Meta order-value job receives only `DATABASE_URL`, `META_ADS_APP_SECRET`, and `META_ADS_ENCRYPTION_KEY` from Secret Manager, while access tokens remain encrypted in application storage instead of being copied into environment files.

For Meta deterministic view/impression ingestion specifically:

- `staging.env` and `production.env` keep `META_ADS_DETERMINISTIC_SCHEDULER_PAUSED="false"` so scheduled ingestion runs after deploy.
- `dev.env` keeps the deterministic scheduler paused by default.
- the deterministic job receives only `DATABASE_URL`, `META_ADS_APP_SECRET`, and `META_ADS_ENCRYPTION_KEY` from Secret Manager.
- initial backfill breadth is controlled by `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS`; routine catch-up is controlled by `META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS`.
- use `docs/runbooks/meta-deterministic-ingestion.md` for restart, pause, and backfill procedures.

Use `sh infra/cloud-run/scheduler.sh <environment> meta-order-value <status|pause|resume>` for the order-value operational toggle without redeploying. Use `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic <status|pause|resume>` for deterministic view/impression ingestion.

Check that the job description still shows command `npm`, args `run,ga4:ingest:start`, and `maxRetries: 0`.

The API service is configured for larger raw JSON ingestion by combining:

- Cloud Run sizing from the environment files
- env-driven Express parser limits for `/track`, general JSON APIs, and Shopify webhooks
- lower service concurrency than the Cloud Run default so request fan-in stays closer to the Cloud SQL pool size

Recommended starting points in this repository:

- API staging: `2` vCPU, `2Gi`, concurrency `16`, timeout `900s`
- API production: `2` vCPU, `2Gi`, concurrency `24`, timeout `900s`
- Worker staging: `2` vCPU, `1Gi`, concurrency `2`, timeout `900s`
- Worker production: `2` vCPU, `1Gi`, concurrency `4`, timeout `900s`
- request parser limits: `20mb` for API JSON, tracking JSON, and Shopify raw webhook bodies

Keep request parser limits below the Cloud Run hard request-body ceiling. Cloud Run still rejects requests above its platform limit even if the app parser limit is higher.

## Staging Verification

After deploying staging or production, run the smoke-test helper:

`sh infra/cloud-run/smoke-test.sh <staging|production>`

The smoke helper now gates rollout promotion on `/api/reporting/meta-order-value` instead of the generic reporting summary endpoint. The check validates:

- unauthenticated access is rejected with `401`
- authenticated access succeeds with a bounded `startDate` and `endDate` query
- the response includes the expected JSON contract surface: `scope.organizationId`, `range`, `pagination`, `totals`, and `rows`

Capture the smoke-test output and the exact date window used as rollout evidence before staging sign-off and before production promotion.
