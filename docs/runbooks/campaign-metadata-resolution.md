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

## Attributed Meta ID Refresh

Use this path when reporting or attribution contains Meta campaign or ad set ids whose display metadata is missing or stale in `ad_platform_entity_metadata`. Example impacted campaign id: `120251699446190386`.

First verify the impacted campaign/ad set scope from spend rows. Replace the date window with the affected attribution window:

```sql
SELECT DISTINCT
  account_id,
  campaign_id,
  campaign_name,
  adset_id,
  adset_name
FROM meta_ads_daily_spend
WHERE report_date BETWEEN DATE 'DATE_START' AND DATE 'DATE_END'
  AND campaign_id = '120251699446190386'
ORDER BY account_id, campaign_id, adset_id;
```

Run the history backfill when spend rows already contain the missing names. This updates campaign, ad set, and ad metadata from historical spend projections:

```bash
DATABASE_URL="<database-url>" npm run campaign-metadata:backfill -- \
  --mode history \
  --start-date DATE_START \
  --end-date DATE_END \
  --requested-by todd-devops-meta-attributed-id-refresh \
  --worker-id meta-attributed-id-metadata-history \
  --dry-run true

DATABASE_URL="<database-url>" npm run campaign-metadata:backfill -- \
  --mode history \
  --start-date DATE_START \
  --end-date DATE_END \
  --requested-by todd-devops-meta-attributed-id-refresh \
  --worker-id meta-attributed-id-metadata-history \
  --dry-run false
```

Run the API refresh when spend rows do not have the current campaign name or the account needs fresh API-confirmed metadata. Use a date window instead of only `--campaign-ids` when ad set rows are also impacted; the campaign-id-only Meta path refreshes matching campaign rows, while a date-scoped account refresh can fetch the account's ad sets and ads too.

```bash
DATABASE_URL="<database-url>" npm run campaign-metadata:backfill -- \
  --mode api-refresh \
  --start-date DATE_START \
  --end-date DATE_END \
  --platforms meta_ads \
  --requested-by todd-devops-meta-attributed-id-refresh \
  --worker-id meta-attributed-id-metadata-api \
  --dry-run true

DATABASE_URL="<database-url>" npm run campaign-metadata:backfill -- \
  --mode api-refresh \
  --start-date DATE_START \
  --end-date DATE_END \
  --platforms meta_ads \
  --requested-by todd-devops-meta-attributed-id-refresh \
  --worker-id meta-attributed-id-metadata-api \
  --dry-run false
```

If only the campaign display name is affected and ad set metadata is not in scope, a campaign-id-only API refresh is acceptable:

```bash
DATABASE_URL="<database-url>" npm run campaign-metadata:backfill -- \
  --mode api-refresh \
  --platforms meta_ads \
  --campaign-ids 120251699446190386 \
  --requested-by todd-devops-meta-attributed-id-refresh \
  --worker-id meta-attributed-id-metadata-api \
  --dry-run false
```

Confirm the metadata table contains matching Meta campaign and ad set rows:

```sql
SELECT
  platform,
  account_id,
  entity_type,
  entity_id,
  latest_name,
  last_seen_at,
  updated_at
FROM ad_platform_entity_metadata
WHERE platform = 'meta_ads'
  AND (
    entity_id = '120251699446190386'
    OR entity_id IN (
      SELECT DISTINCT adset_id
      FROM meta_ads_daily_spend
      WHERE campaign_id = '120251699446190386'
        AND adset_id IS NOT NULL
        AND report_date BETWEEN DATE 'DATE_START' AND DATE 'DATE_END'
    )
  )
ORDER BY account_id, entity_type, entity_id;
```

After the refresh/backfill, trigger or inspect reporting paths that call metadata resolution and confirm unresolved ids are visible in `campaign_metadata_resolution_coverage` logs. Expected unresolved evidence includes nonzero `unresolvedCount` and sampled `unresolvedEntityIds`; these logs are the existing coverage surface for ids that still cannot be matched after refresh.

## Verification

After a scheduler change or deploy, verify:

- `campaign_metadata_sync_job_lifecycle` logs show the expected `requestedBy` value
- `campaign_metadata_freshness_snapshot` shows stale counts stabilizing or falling
- `campaign_metadata_resolution_coverage` stays consistent with recent reporting windows
- duplicate entity ids remain isolated by `(platform, account_id, entity_type, entity_id)` rather than cross-platform name leakage
