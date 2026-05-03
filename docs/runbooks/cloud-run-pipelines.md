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

