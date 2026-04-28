# Cloud Run Deployment

This directory contains the operational scripts and environment definitions for deploying the ROAS Radar API, dashboard, attribution worker, migration job, and scheduled pipeline jobs to Google Cloud Run.

## Topology

The deployment flow assumes ten deployable workloads plus six Cloud Scheduler triggers:

- `roas-radar-api`: public Cloud Run service for `/track`, Shopify webhooks, and authenticated reporting APIs.
- `roas-radar-dashboard`: public Cloud Run service for the React reporting dashboard.
- `roas-radar-attribution-worker`: internal Cloud Run service for attribution and asynchronous processing.
- `roas-radar-migrate`: Cloud Run Job that runs `npm run db:migrate:start` with elevated database credentials.
- `roas-radar-meta-ads-sync`: Cloud Run Job that runs `npm run meta-ads:sync:start` once per invocation.
- `roas-radar-google-ads-sync`: Cloud Run Job that runs `npm run google-ads:sync:start` once per invocation.
- `roas-radar-session-retention`: Cloud Run Job that runs `npm run session-attribution:retention:start` to prune expired attribution-session records.
- `roas-radar-data-quality`: Cloud Run Job that runs `npm run data-quality:check:start` once per invocation.
- `roas-radar-identity-graph-backfill`: Cloud Run Job that runs `npm run identity:backfill-graph:start` over a recent window to reconcile graph attachments and catch missed identity stitching.
- `roas-radar-order-attribution-materialization`: Cloud Run Job that runs `npm run attribution:materialization:start` over a recent order window to recover attribution and refresh reporting aggregates.
- `roas-radar-meta-ads-sync-scheduler`: Cloud Scheduler job that invokes the Meta Ads Cloud Run Job.
- `roas-radar-google-ads-sync-scheduler`: Cloud Scheduler job that invokes the Google Ads Cloud Run Job.
- `roas-radar-session-retention-scheduler`: Cloud Scheduler job that invokes the session-retention Cloud Run Job.
- `roas-radar-data-quality-scheduler`: Cloud Scheduler job that invokes the data-quality Cloud Run Job.
- `roas-radar-identity-graph-backfill-scheduler`: Cloud Scheduler job that invokes the identity-graph backfill Cloud Run Job.
- `roas-radar-order-attribution-materialization-scheduler`: Cloud Scheduler job that invokes the order-attribution materialization Cloud Run Job.

The API and worker use the `roas_app` PostgreSQL login. The migration job uses the `roas_migrator` PostgreSQL login. Do not reuse the migrator credential in long-lived application services.

Each recurring job now runs as its own service account, and Cloud Scheduler uses a dedicated invoker identity with `roles/run.invoker`. That keeps ads sync, data quality, identity reconciliation, and attribution materialization from sharing unnecessary secret access.

## Required Secrets

The deploy script expects the following Secret Manager secrets to exist for each environment:

- `DATABASE_URL`: runtime DSN for the API and worker (`roas_app` user).
- `MIGRATOR_DATABASE_URL`: DSN for the migration job (`roas_migrator` user).
- `REPORTING_API_TOKEN`
- `SHOPIFY_WEBHOOK_SECRET`
- `SHOPIFY_APP_API_KEY`
- `SHOPIFY_APP_API_SECRET`
- `SHOPIFY_APP_ENCRYPTION_KEY`
- `META_ADS_APP_SECRET`
- `META_ADS_ENCRYPTION_KEY`
- `GOOGLE_ADS_ENCRYPTION_KEY`

The environment files also carry non-secret runtime settings that must be populated before deployment, including:

- `TRACKING_ALLOWED_ORIGINS`
- `API_JSON_BODY_LIMIT`
- `TRACKING_BODY_LIMIT`
- `SHOPIFY_WEBHOOK_BODY_LIMIT`
- `SHOPIFY_APP_BASE_URL`
- `SHOPIFY_APP_API_VERSION`
- `SHOPIFY_APP_SCOPES`
- `SHOPIFY_APP_POST_INSTALL_REDIRECT_URL`
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
- `GOOGLE_ADS_JOB_NAME`
- `META_ADS_SCHEDULER_JOB_NAME`
- `GOOGLE_ADS_SCHEDULER_JOB_NAME`
- `RETENTION_JOB_NAME`
- `DATA_QUALITY_JOB_NAME`
- `IDENTITY_GRAPH_BACKFILL_JOB_NAME`
- `ORDER_ATTRIBUTION_MATERIALIZATION_JOB_NAME`
- `RETENTION_SCHEDULER_JOB_NAME`
- `DATA_QUALITY_SCHEDULER_JOB_NAME`
- `IDENTITY_GRAPH_BACKFILL_SCHEDULER_JOB_NAME`
- `ORDER_ATTRIBUTION_MATERIALIZATION_SCHEDULER_JOB_NAME`
- `RETENTION_JOB_SERVICE_ACCOUNT_NAME`
- `META_ADS_JOB_SERVICE_ACCOUNT_NAME`
- `GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME`
- `DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME`
- `IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME`
- `ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME`
- `SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME`
- `ADS_SYNC_DATABASE_POOL_MAX`
- `ADS_SYNC_TIME_ZONE`
- `META_ADS_SYNC_SCHEDULE`
- `GOOGLE_ADS_SYNC_SCHEDULE`
- `RETENTION_SCHEDULE`
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

## First-Time Setup

1. Provision Cloud SQL and private networking from `infra/cloud-sql/`.
2. Run `infra/cloud-run/bootstrap-iam.sh ENVIRONMENT` to create service accounts and grant IAM roles.
3. Create the environment secrets in Secret Manager.
4. Populate `infra/cloud-run/environments/dev.env`, `staging.env`, and `production.env`.
5. Deploy with `infra/cloud-run/deploy.sh ENVIRONMENT`.
6. Apply monitoring with `infra/monitoring/apply.sh ENVIRONMENT`.
7. Validate the scheduled jobs and schedulers with `docs/runbooks/cloud-run-pipelines.md`.

## Deployment

For one-off environment deploys, run `infra/cloud-run/deploy.sh dev`, `infra/cloud-run/deploy.sh staging`, or `infra/cloud-run/deploy.sh production`.

`infra/cloud-run/deploy.sh` now deploys the migrator job first, runs schema migration before any service rollout, then deploys the API, worker, dashboard, jobs, and schedulers. If `DEPLOY_METADATA_FILE` is set, the script also records the previous and newly deployed Cloud Run revisions for the API, worker, and dashboard so rollback can route traffic back to the prior revision quickly.

For staged promotion, use `infra/cloud-run/promote.sh <dev|staging|production>`. The promotion script:

- deploys each environment in order (`dev -> staging -> production`)
- runs migrations before shifting service traffic in each environment
- executes smoke tests after each environment deploy
- can run a staging rollback drill by toggling `RUN_STAGING_ROLLBACK_DRILL=true`
- persists rollout metadata in `infra/cloud-run/.deploy-state/`

The Cloud Build trigger should run as the environment deployer service account created by `bootstrap-iam.sh`:

- `roas-radar-deployer-dev@<project>.iam.gserviceaccount.com` for dev
- `roas-radar-deployer-staging@<project>.iam.gserviceaccount.com` for staging
- `roas-radar-deployer-prod@<project>.iam.gserviceaccount.com` for production

Recommended trigger settings:

- event: push to branch
- branch regex: `^main$`
- region: the same region used by Cloud Run and Artifact Registry
- build config: `cloudbuild.staging.yaml`

Use `cloudbuild.release.yaml` for the full staged promotion pipeline. Its defaults promote through production, run migrations, apply monitoring, and require a successful staging rollback drill before production deployment continues. Use substitutions to stop at staging or disable the drill when needed.

## Scheduled Pipelines

The default daily sequence is:

1. session retention at `0 3 * * *`
2. data quality at `20 3 * * *`
3. identity graph backfill at `35 3 * * *`
4. order attribution materialization at `50 3 * * *`

Meta Ads and Google Ads sync remain hourly schedulers. If one scheduled job is unhealthy, pause only that scheduler entry and leave the attribution worker service running because it also drains the live attribution queue.

## Large Payload Throughput

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

