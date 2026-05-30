# Meta Deterministic Ingestion Runbook

Use this runbook for the Meta deterministic view/impression Cloud Run Job, `roas-radar-meta-deterministic-sync-*`.

## Triggers

- sustained `meta_ads_deterministic_worker_failed` or `meta_ads_deterministic_sync_job_failed` log events
- flatlined `roas_meta_deterministic_rows_fetched`, `roas_meta_deterministic_raw_rows_upserted`, or `roas_meta_deterministic_aggregate_rows_upserted`
- `meta_ads_deterministic_api_reconciliation` data-quality mismatches

## Immediate Checks

1. Check the scheduler state: `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic status`.
2. Review Cloud Logging for `jsonPayload.event=("meta_ads_deterministic_worker_started" OR "meta_ads_deterministic_sync_job_completed" OR "meta_ads_deterministic_sync_job_failed")`.
3. Confirm the job service account is `META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME` and that it can read only `DATABASE_URL`, `META_ADS_APP_SECRET`, and `META_ADS_ENCRYPTION_KEY`.
4. Inspect whether failures are isolated to one ad account, date, entity type, or event type before widening backfill windows.

## Restart

1. If a scheduler invocation failed transiently, rerun the job:
   `gcloud run jobs execute roas-radar-meta-deterministic-sync-<environment> --project <project> --region <region> --wait`
2. If failures continue, pause the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic pause`
3. Resume only after a manual job execution completes and emits `meta_ads_deterministic_worker_completed`.

## Backfill

1. Set `META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS` to the required bounded window in the target environment file.
2. Rerun `sh infra/cloud-run/deploy.sh <environment>` so the Cloud Run Job picks up the backfill window.
3. Execute the deterministic job manually and wait for completion.
4. Restore the normal lookback value and redeploy before resuming the scheduler.

Keep `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS` at the wider initial planning window for new account/entity checkpoints. Use `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` only as an emergency kill switch when the job should stay deployed but do no extraction.

## Escalation

- Escalate to application engineering if the job repeatedly fails for the same planned date or cannot persist raw deterministic evidence.
- Escalate to the ads owner if Meta returns permission errors or empty deterministic view/impression fields for active campaigns.
