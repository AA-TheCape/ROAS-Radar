# Cloud Run Pipelines

## Managed Workloads

The Cloud Run deploy pipeline manages:

- API service: `API_SERVICE_NAME`
- Dashboard service: `DASHBOARD_SERVICE_NAME`
- Attribution worker service: `WORKER_SERVICE_NAME`
- Migrator job: `MIGRATOR_JOB_NAME`
- Meta Ads metadata refresh job: `META_ADS_METADATA_JOB_NAME`
- Google Ads metadata refresh job: `GOOGLE_ADS_METADATA_JOB_NAME`

Metadata refresh runs are scheduled workloads. They are not part of the normal ad sync queue path.

## Standard Deploy

Deploy the baseline Cloud Run workloads with:

```bash
./infra/cloud-run/deploy.sh
```

The deploy script reads the active `infra/cloud-run/environments/*.env` file and manages both services and jobs, including:

- `API_SERVICE_NAME`
- `DASHBOARD_SERVICE_NAME`
- `WORKER_SERVICE_NAME`
- `MIGRATOR_JOB_NAME`
- `META_ADS_METADATA_JOB_NAME`
- `GOOGLE_ADS_METADATA_JOB_NAME`

## Metadata Refresh Scheduler Controls

Cloud Scheduler owns the recurring metadata refresh triggers for the dedicated Cloud Run jobs:

- Meta Ads scheduler: `META_ADS_METADATA_SCHEDULER_NAME`
- Google Ads scheduler: `GOOGLE_ADS_METADATA_SCHEDULER_NAME`

Per environment, define:

- `META_ADS_METADATA_SCHEDULER_NAME`
- `META_ADS_METADATA_SCHEDULE`
- `META_ADS_METADATA_SCHEDULER_ENABLED`
- `META_ADS_METADATA_REFRESH_REQUESTED_BY`
- `GOOGLE_ADS_METADATA_SCHEDULER_NAME`
- `GOOGLE_ADS_METADATA_SCHEDULE`
- `GOOGLE_ADS_METADATA_SCHEDULER_ENABLED`
- `GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY`

`infra/cloud-run/deploy.sh` creates the Cloud Scheduler jobs with `gcloud scheduler jobs create http`, points them at the metadata refresh job entrypoints, and applies the scheduler identity used in `requestedBy` logging.

## Create Or Update Schedulers

After the Cloud Run jobs exist, create or refresh the metadata schedulers by rerunning:

```bash
./infra/cloud-run/deploy.sh
```

The script will configure:

- `META_ADS_METADATA_JOB_NAME` behind `META_ADS_METADATA_SCHEDULER_NAME`
- `GOOGLE_ADS_METADATA_JOB_NAME` behind `GOOGLE_ADS_METADATA_SCHEDULER_NAME`

This keeps scheduled metadata refresh outside the normal `meta-ads` and `google-ads` sync queues.

## Pause And Resume

To pause scheduled metadata refresh without disabling the Cloud Run jobs themselves:

```bash
gcloud scheduler jobs pause "$META_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
gcloud scheduler jobs pause "$GOOGLE_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
```

To resume them:

```bash
gcloud scheduler jobs resume "$META_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
gcloud scheduler jobs resume "$GOOGLE_ADS_METADATA_SCHEDULER_NAME" --location "$GCP_REGION"
```

Use pause when investigating noisy upstream failures or when a platform-specific incident requires holding only one metadata path. Resume after the incident is cleared and verify fresh `campaign_metadata_sync_job_lifecycle` completion logs.

## Operator Checks

After deploy or scheduler changes, verify:

- the Cloud Run jobs for `META_ADS_METADATA_JOB_NAME` and `GOOGLE_ADS_METADATA_JOB_NAME` exist
- the Cloud Scheduler jobs are present and target the expected region
- `META_ADS_METADATA_REFRESH_REQUESTED_BY` and `GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY` appear in structured logs as `requestedBy`
- the normal ad sync workers remain unchanged and do not trigger metadata refresh API calls on their own
