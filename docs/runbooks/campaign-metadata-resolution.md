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

