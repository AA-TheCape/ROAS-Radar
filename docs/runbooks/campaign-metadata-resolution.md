# Campaign Metadata Resolution

## Scope

This runbook covers the latest-name metadata lookup surface and the scheduled metadata refresh jobs that keep `ad_platform_entity_metadata` current for:

- `meta_ads`
- `google_ads`

The scheduled refresh path is the Cloud Run metadata refresh jobs, not the normal spend sync workers.

## Required Scheduler Inputs

Per environment, define:

- `META_ADS_METADATA_REFRESH_REQUESTED_BY`
- `GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY`
- `META_ADS_METADATA_SCHEDULER_NAME`
- `GOOGLE_ADS_METADATA_SCHEDULER_NAME`

Those requested-by values are emitted on `campaign_metadata_sync_job_lifecycle` logs so dashboards and incident responders can distinguish Cloud Scheduler executions from manual operator runs.

## Normal Signals

Review these logs and metrics:

- `campaign_metadata_sync_job_lifecycle`
- `campaign_metadata_freshness_snapshot`
- `campaign_metadata_resolution_coverage`
- `logging.googleapis.com/user/roas_metadata_refresh_jobs`
- `logging.googleapis.com/user/roas_campaign_metadata_sync_latency_ms`
- `logging.googleapis.com/user/roas_campaign_metadata_stale_count`

Healthy expectations:

- scheduled jobs complete without repeated `stage="failed"` events
- `requestedBy` matches the environment-specific scheduler value
- stale entity counts return toward zero after successful runs
- resolution hit rate stays stable after deploys and backfills

## Triage

1. Confirm the scheduler is enabled when it should be:
   - `META_ADS_METADATA_SCHEDULER_NAME`
   - `GOOGLE_ADS_METADATA_SCHEDULER_NAME`
2. Confirm the Cloud Scheduler caller identity is the expected source in `campaign_metadata_sync_job_lifecycle`:
   - `META_ADS_METADATA_REFRESH_REQUESTED_BY`
   - `GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY`
3. Check for recent `stage="failed"` events and note `platform`, `workerId`, and `requestedBy`.
4. Verify stale rows are scoped correctly by `(platform, account_id, entity_type, entity_id)` before treating a name collision as a refresh bug.

## Cloud Scheduler Operations

Create or refresh the schedulers through `./infra/cloud-run/deploy.sh`. The deploy workflow is the supported path because it keeps scheduler settings aligned with the environment templates and the Cloud Run job names.

To pause one platform without affecting the other:

```bash
gcloud scheduler jobs pause "$META_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
gcloud scheduler jobs pause "$GOOGLE_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
```

To resume:

```bash
gcloud scheduler jobs resume "$META_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
gcloud scheduler jobs resume "$GOOGLE_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
```

Use the per-platform pause or resume controls during upstream quota incidents, rollout verification, or backfill windows where only one metadata source should run.

## Verification

After a scheduler change or deploy, verify:

- `campaign_metadata_sync_job_lifecycle` logs show the expected `requestedBy` value
- `campaign_metadata_freshness_snapshot` shows stale counts stabilizing or falling
- `campaign_metadata_resolution_coverage` stays consistent with recent reporting windows
- duplicate entity ids remain isolated by `(platform, account_id, entity_type, entity_id)` rather than cross-platform name leakage
