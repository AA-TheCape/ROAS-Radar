# Cloud Run Deploy Artifacts

`infra/cloud-run/deploy.sh` deploys the API service, dashboard service, worker service, migrator job, and the dedicated metadata refresh jobs for Meta Ads and Google Ads.

## Environment Files

Each environment file under `infra/cloud-run/environments/*.env` is a real shell-compatible config file consumed by:

- `bootstrap-iam.sh`
- `deploy.sh`
- `smoke-test.sh`
- `rollback.sh`

The metadata refresh rollout depends on these per-platform variables:

- `META_ADS_METADATA_JOB_NAME`
- `META_ADS_METADATA_SCHEDULER_NAME`
- `META_ADS_METADATA_SCHEDULE`
- `META_ADS_METADATA_TIME_ZONE`
- `META_ADS_METADATA_SCHEDULER_ENABLED`
- `META_ADS_METADATA_REFRESH_REQUESTED_BY`
- `GOOGLE_ADS_METADATA_JOB_NAME`
- `GOOGLE_ADS_METADATA_SCHEDULER_NAME`
- `GOOGLE_ADS_METADATA_SCHEDULE`
- `GOOGLE_ADS_METADATA_TIME_ZONE`
- `GOOGLE_ADS_METADATA_SCHEDULER_ENABLED`
- `GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY`

`*_SCHEDULER_ENABLED=true` means `deploy.sh` resumes the Cloud Scheduler job after it updates the target. `false` means the scheduler is created or updated and then paused.

## Deploy Flow

Run:

